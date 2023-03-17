package SurfTest

import (
	// "cse224/proj5/pkg/surfstore"
	"context"
	"cse224/proj5/pkg/surfstore"
	"testing"
	"time"

	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

func TestRaftLogsCorrectlyOverwritten(t *testing.T) {
	//Setup
	cfgPath := "./config_files/3nodes.txt"
	test := InitTest(cfgPath)
	defer EndTest(test)

	// TEST
	if len(test.Clients) != 3 {
		panic("should have 3 servers")
	}
	test.Clients[0].SetLeader(test.Context, &emptypb.Empty{})

	// heartbeat
	for _, server := range test.Clients {
		server.SendHeartbeat(test.Context, &emptypb.Empty{})
	}

	test.Clients[1].Crash(test.Context, &emptypb.Empty{})
	test.Clients[2].Crash(test.Context, &emptypb.Empty{})

	update1 := &surfstore.FileMetaData{
		Filename: "test456.txt", Version: 1, BlockHashList: []string{"4", "5", "6"},
	}
	update2 := &surfstore.FileMetaData{
		Filename: "test123.txt", Version: 1, BlockHashList: []string{"1", "2", "3"},
	}

	ctx, cancel := context.WithCancel(test.Context)
	defer cancel()
	go test.Clients[0].UpdateFile(ctx, update1)
	time.Sleep(100 * time.Millisecond)
	go test.Clients[0].UpdateFile(ctx, update2)
	time.Sleep(100 * time.Millisecond)

	// heartbeat
	for _, server := range test.Clients {
		server.SendHeartbeat(test.Context, &emptypb.Empty{})
	}

	isLeader := true
	term := int64(1)
	_, err := CheckInternalState(
		&isLeader,
		&term,
		[]*surfstore.UpdateOperation{{Term: 1, FileMetaData: update1}, {Term: 1, FileMetaData: update2}},
		map[string]*surfstore.FileMetaData{},
		test.Clients[0],
		test.Context,
	)
	if err != nil {
		t.Fatalf("Failed to check state 1: %v", err)
	}

	test.Clients[0].Crash(ctx, &emptypb.Empty{})
	test.Clients[1].Restore(ctx, &emptypb.Empty{})
	test.Clients[2].Restore(ctx, &emptypb.Empty{})
	test.Clients[1].SetLeader(ctx, &emptypb.Empty{})

	update3 := &surfstore.FileMetaData{
		Filename: "test789.txt", Version: 1, BlockHashList: []string{"7", "8", "9"},
	}
	test.Clients[1].UpdateFile(ctx, update3)
	// heartbeat
	for _, server := range test.Clients {
		server.SendHeartbeat(test.Context, &emptypb.Empty{})
	}

	isLeader = true
	term = int64(2)
	_, err = CheckInternalState(
		&isLeader,
		&term,
		[]*surfstore.UpdateOperation{{Term: 2, FileMetaData: update3}},
		map[string]*surfstore.FileMetaData{"test789.txt": update3},
		test.Clients[1],
		test.Context,
	)
	if err != nil {
		t.Fatalf("Failed to check state 2: %v", err)
	}

	test.Clients[0].Restore(ctx, &emptypb.Empty{})
	// heartbeat
	for _, server := range test.Clients {
		server.SendHeartbeat(test.Context, &emptypb.Empty{})
	}

	isLeader = false
	term = int64(2)
	_, err = CheckInternalState(
		&isLeader,
		&term,
		[]*surfstore.UpdateOperation{{Term: 2, FileMetaData: update3}},
		map[string]*surfstore.FileMetaData{"test789.txt": update3},
		test.Clients[0],
		test.Context,
	)
	if err != nil {
		t.Fatalf("Failed to check state 3: %v", err)
	}
}
