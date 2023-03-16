package surfstore

import (
	context "context"
	"sync"
	"time"

	"google.golang.org/grpc"
	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

// TODO Add fields you need here
type RaftSurfstore struct {
	isLeader      bool
	isLeaderMutex *sync.RWMutex
	term          int64
	log           []*UpdateOperation

	metaStore *MetaStore

	/*------------- From discussion -------------*/
	id             int64
	peers          []string
	pendingCommits []*chan bool
	commitIndex    int64
	lastApplied    int64
	// self
	nextIndex  []int
	matchIndex []int
	logMutex   *sync.RWMutex

	/*--------------- Chaos Monkey --------------*/
	isCrashed      bool
	isCrashedMutex *sync.RWMutex
	UnimplementedRaftSurfstoreServer
}

func (s *RaftSurfstore) GetFileInfoMap(ctx context.Context, empty *emptypb.Empty) (*FileInfoMap, error) {
	// panic("todo")
	if s.isCrashed {
		return nil, ERR_SERVER_CRASHED
	}

	if !s.isLeader {
		return nil, ERR_NOT_LEADER
	}
	responses := make(chan bool, len(s.peers)-1)
	totalGet := 1
	for idx, addr := range s.peers {
		if int64(idx) == s.id {
			continue
		}
		go s.checkAlive(addr, responses)
	}
	for {
		success := <-responses
		if success {
			totalGet++
		}
		if totalGet > len(s.peers)/2 {
			return s.metaStore.GetFileInfoMap(ctx, empty)
		}
	}
	return nil, nil
}

func (s *RaftSurfstore) GetBlockStoreMap(ctx context.Context, hashes *BlockHashes) (*BlockStoreMap, error) {
	// panic("todo")
	if s.isCrashed {
		return nil, ERR_SERVER_CRASHED
	}

	if !s.isLeader {
		return nil, ERR_NOT_LEADER
	}
	responses := make(chan bool, len(s.peers)-1)
	totalGet := 1
	for idx, addr := range s.peers {
		if int64(idx) == s.id {
			continue
		}
		go s.checkAlive(addr, responses)
	}
	for {
		success := <-responses
		if success {
			totalGet++
		}
		if totalGet > len(s.peers)/2 {
			return s.metaStore.GetBlockStoreMap(ctx, hashes)
		}
	}
	return nil, nil
}

func (s *RaftSurfstore) GetBlockStoreAddrs(ctx context.Context, empty *emptypb.Empty) (*BlockStoreAddrs, error) {
	// panic("todo")
	if s.isCrashed {
		return nil, ERR_SERVER_CRASHED
	}

	if !s.isLeader {
		return nil, ERR_NOT_LEADER
	}
	responses := make(chan bool, len(s.peers)-1)
	totalGet := 1
	for idx, addr := range s.peers {
		if int64(idx) == s.id {
			continue
		}
		go s.checkAlive(addr, responses)
	}
	for {
		success := <-responses
		if success {
			totalGet++
		}
		if totalGet > len(s.peers)/2 {
			return s.metaStore.GetBlockStoreAddrs(ctx, empty)
		}
	}
	return nil, nil
}

func (s *RaftSurfstore) checkAlive(addr string, responses chan bool) {
	for {
		conn, err := grpc.Dial(addr, grpc.WithInsecure())
		if err != nil {
			continue
		}
		client := NewRaftSurfstoreClient(conn)
		dummyAppendEntryInput := &AppendEntryInput{
			Term:         s.term,
			PrevLogTerm:  -1,
			PrevLogIndex: -1,
			Entries:      make([]*UpdateOperation, 0),
			LeaderCommit: s.commitIndex,
		}

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_, err = client.AppendEntries(ctx, dummyAppendEntryInput)
		conn.Close()
		if err == nil {
			responses <- true
			return
		}
	}
}

func (s *RaftSurfstore) UpdateFile(ctx context.Context, filemeta *FileMetaData) (*Version, error) {
	//panic("todo")
	//fmt.Println("enter update file")
	if s.isCrashed {
		return nil, ERR_SERVER_CRASHED
	}
	if !s.isLeader {
		return nil, ERR_NOT_LEADER
	}
	// append entry to our log
	s.logMutex.Lock()
	s.log = append(s.log, &UpdateOperation{
		Term:         s.term,
		FileMetaData: filemeta,
	})
	s.logMutex.Unlock()
	commitChan := make(chan bool)
	// send entry to all followers in parallel
	go s.sendToAllFollowersInParallel(ctx, commitChan)
	// keep trying indefinitely (even after responding) ** rely on sendheartbeat

	// commit the entry once majority of followers have it in their log
	totalCommit := 1
	for {
		commit := <-commitChan
		// once commited, apply to the state machine
		if commit {
			totalCommit++
		}
		if totalCommit > len(s.peers)/2 {
			s.lastApplied = s.commitIndex
			return s.metaStore.UpdateFile(ctx, filemeta)
		}
	}
	return nil, nil
}

func (s *RaftSurfstore) sendToAllFollowersInParallel(ctx context.Context, commitChan chan bool) {
	// send entry to all my followers and count the replies
	responses := make(chan bool, len(s.peers)-1)
	for idx, addr := range s.peers {
		if int64(idx) == s.id {
			continue
		}
		go s.sendToFollower(ctx, idx, addr, responses)
	}
	totalAppends := 1
	newCommitIndex := s.commitIndex + 1
	// wait in loop for responses
	for {
		res := <-responses
		if res {
			totalAppends++
		}
		if totalAppends > len(s.peers)/2 {
			s.commitIndex = newCommitIndex
			commitChan <- true
			break
		}
	}

	//fmt.Println("get so much responses, this should not happend")

	for totalAppends < len(s.peers) {
		res := <-responses
		if res {
			totalAppends++
		}
	}
	//fmt.Println("Now has sent to all the followers")
}

func (s *RaftSurfstore) sendToFollower(ctx context.Context, idx int, addr string, responses chan bool) {
	realAppendEntryInput := AppendEntryInput{
		Term:         s.term,
		PrevLogTerm:  -1,
		PrevLogIndex: -1,
		Entries:      s.log,
		LeaderCommit: s.commitIndex,
	}
	/*
		realAppendEntryInput.PrevLogIndex = int64(len(s.log) - 2)
		if realAppendEntryInput.PrevLogIndex != -1 {
			realAppendEntryInput.PrevLogTerm = s.log[len(s.log)-2].Term
		}
	*/
	realAppendEntryInput.PrevLogIndex = int64(s.nextIndex[idx] - 1)
	if s.nextIndex[idx]-1 == -1 {
		realAppendEntryInput.PrevLogTerm = -1
	} else {
		realAppendEntryInput.PrevLogTerm = s.log[s.nextIndex[idx]-1].Term
	}

	conn, _ := grpc.Dial(addr, grpc.WithInsecure())
	client := NewRaftSurfstoreClient(conn)

	output, err := client.AppendEntries(ctx, &realAppendEntryInput)
	conn.Close()

	if err == nil {
		if output.Success {
			s.nextIndex[idx] = len(s.log)
			s.matchIndex[idx] = len(s.log) - 1
		} else if output.Term > s.term {
			s.isLeaderMutex.Lock()
			s.isLeader = false
			s.term = output.Term
			s.isLeaderMutex.Unlock()
		} else {
			s.nextIndex[idx] = int(output.MatchedIndex) + 1
		}
		//fmt.Println("append success and return resp")
		responses <- true
	}
}

// 1. Reply false if term < currentTerm (§5.1)
// 2. Reply false if log doesn’t contain an entry at prevLogIndex whose term
// matches prevLogTerm (§5.3)
// 3. If an existing entry conflicts with a new one (same index but different
// terms), delete the existing entry and all that follow it (§5.3)
// 4. Append any new entries not already in the log
// 5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index
// of last new entry)
func (s *RaftSurfstore) AppendEntries(ctx context.Context, input *AppendEntryInput) (*AppendEntryOutput, error) {
	//panic("todo")
	if s.isCrashed {
		//fmt.Println("here should crash")
		return nil, ERR_SERVER_CRASHED
	}

	output := &AppendEntryOutput{
		ServerId:     s.id,
		Term:         s.term,
		Success:      false,
		MatchedIndex: -1,
	}

	if input.Term > s.term {
		s.isLeaderMutex.Lock()
		s.isLeader = false
		s.term = input.Term
		s.isLeaderMutex.Unlock()
		// return output, nil
	}

	//lastNewIndex := -1

	if len(input.Entries) > 0 {
		//fmt.Println("I'm receiver")
		//fmt.Println(input.LeaderCommit)
		//fmt.Println(input.PrevLogIndex)
		// 1
		if input.Term < s.term {
			return output, nil
		}
		// 2
		// have some conflict, to get the last matched index
		if len(s.log) < int(input.PrevLogIndex+1) {
			output.MatchedIndex = int64(len(s.log) - 1)
			return output, nil
		}
		if input.PrevLogIndex != -1 && s.log[input.PrevLogIndex].Term != input.PrevLogTerm {
			for i := input.PrevLogIndex - 1; i >= 0; i-- {
				if s.log[i].Term != s.log[input.PrevLogIndex].Term {
					output.MatchedIndex = i
					break
				}
			}
			return output, nil
		}
		// success
		output.Success = true
		s.logMutex.Lock()
		// 3
		for idx, existingEntry := range s.log {
			if existingEntry.Term != input.Entries[idx].Term {
				for idx2 := idx; s.log[idx2] != nil; idx2++ {
					s.log[idx2] = nil
					if len(s.log)-1 < idx2+1 {
						break
					}
				}
				break
			}
		}
		// 4
		//lastNewIndex = len(s.log) - 1
		for idx := len(s.log); idx < len(input.Entries); idx++ {
			s.log = append(s.log, input.Entries[idx])
			//lastNewIndex = idx
		}
		s.logMutex.Unlock()
		output.MatchedIndex = int64(len(input.Entries) - 1)
	}

	// 5
	if input.LeaderCommit > s.commitIndex {
		if input.LeaderCommit < int64(len(s.log))-1 {
			s.commitIndex = input.LeaderCommit
		} else {
			s.commitIndex = int64(len(s.log)) - 1
		}
		//fmt.Println("now " + s.peers[s.id])
		//fmt.Println(s.commitIndex)
	}

	// s.log = input.Entries
	for s.lastApplied < s.commitIndex {
		s.lastApplied++
		entry := s.log[s.lastApplied]
		s.metaStore.UpdateFile(ctx, entry.FileMetaData)
	}
	return output, nil
}

func (s *RaftSurfstore) SetLeader(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	//panic("todo")
	if s.isCrashed {
		return nil, ERR_SERVER_CRASHED
	}
	s.isLeaderMutex.Lock()
	s.isLeader = true
	s.term++
	s.isLeaderMutex.Unlock()
	s.nextIndex = make([]int, len(s.peers))
	s.matchIndex = make([]int, len(s.peers))

	for idx := range s.peers {
		s.nextIndex[idx] = len(s.log)
		s.matchIndex[idx] = 0
	}
	return &Success{Flag: true}, nil
}

func (s *RaftSurfstore) SendHeartbeat(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	//panic("todo")
	if s.isCrashed {
		return nil, ERR_SERVER_CRASHED
	}
	if !s.isLeader {
		return nil, ERR_NOT_LEADER
	}
	dummyAppendEntryInput := AppendEntryInput{
		Term:         s.term,
		PrevLogTerm:  -1,
		PrevLogIndex: -1,
		Entries:      make([]*UpdateOperation, 0),
		LeaderCommit: s.commitIndex,
	}
	if len(s.log) > 0 {
		dummyAppendEntryInput.PrevLogIndex = int64(len(s.log) - 1)
		dummyAppendEntryInput.PrevLogTerm = s.log[len(s.log)-1].Term
	}
	for idx, addr := range s.peers {
		if int64(idx) == s.id {
			continue
		}
		dummyAppendEntryInput.PrevLogIndex = int64(s.nextIndex[idx] - 1)
		if s.nextIndex[idx]-1 == -1 {
			dummyAppendEntryInput.PrevLogTerm = -1
		} else {
			dummyAppendEntryInput.PrevLogTerm = s.log[s.nextIndex[idx]-1].Term
		}
		conn, _ := grpc.Dial(addr, grpc.WithInsecure())
		client := NewRaftSurfstoreClient(conn)

		output, err := client.AppendEntries(ctx, &dummyAppendEntryInput)
		if err == nil {
			if output.Success {
				s.nextIndex[idx] = len(s.log)
				s.matchIndex[idx] = len(s.log) - 1
			} else if output.Term > s.term {
				s.isLeaderMutex.Lock()
				s.isLeader = false
				s.term = output.Term
				s.isLeaderMutex.Unlock()
			} else {
				s.nextIndex[idx] = int(output.MatchedIndex) + 1
			}
		}
	}
	return &Success{Flag: true}, nil
}

// ========== DO NOT MODIFY BELOW THIS LINE =====================================

func (s *RaftSurfstore) Crash(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	s.isCrashedMutex.Lock()
	s.isCrashed = true
	s.isCrashedMutex.Unlock()

	return &Success{Flag: true}, nil
}

func (s *RaftSurfstore) Restore(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	s.isCrashedMutex.Lock()
	s.isCrashed = false
	s.isCrashedMutex.Unlock()

	return &Success{Flag: true}, nil
}

func (s *RaftSurfstore) GetInternalState(ctx context.Context, empty *emptypb.Empty) (*RaftInternalState, error) {
	fileInfoMap, _ := s.metaStore.GetFileInfoMap(ctx, empty)
	s.isLeaderMutex.RLock()
	state := &RaftInternalState{
		IsLeader: s.isLeader,
		Term:     s.term,
		Log:      s.log,
		MetaMap:  fileInfoMap,
	}
	s.isLeaderMutex.RUnlock()

	return state, nil
}

var _ RaftSurfstoreInterface = new(RaftSurfstore)
