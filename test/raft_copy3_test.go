package SurfTest

import (
	// "cse224/proj5/pkg/surfstore"
	//"fmt"
	emptypb "google.golang.org/protobuf/types/known/emptypb"
	"testing"
)

func TestRaft4(t *testing.T) {
	//Setup
	cfgPath := "./config_files/3nodes.txt"
	test := InitTest(cfgPath)
	defer EndTest(test)

	// TEST
	test.Clients[0].SetLeader(test.Context, &emptypb.Empty{})
	test.Clients[0].SendHeartbeat(test.Context, &emptypb.Empty{})
	test.Clients[1].Crash(test.Context, &emptypb.Empty{})

	worker1 := InitDirectoryWorker("test0", SRC_PATH)
	worker2 := InitDirectoryWorker("test1", SRC_PATH)
	defer worker1.CleanUp()
	defer worker2.CleanUp()

	//clients add different files
	file1 := "multi_file1.txt"
	//file2 := "multi_file1.txt"
	err := worker1.AddFile(file1)
	if err != nil {
		t.FailNow()
	}

	/*
		err = worker2.AddFile(file2)
		if err != nil {
			t.FailNow()
		}
		err = worker2.UpdateFile(file2, "update text")
		if err != nil {
			t.FailNow()
		}
	*/

	//client1 syncs
	err = SyncClient("localhost:8080", "test0", BLOCK_SIZE, cfgPath)
	if err != nil {
		t.Fatalf("Sync failed")
	}

	test.Clients[0].SendHeartbeat(test.Context, &emptypb.Empty{})
	test.Clients[0].Crash(test.Context, &emptypb.Empty{})
	test.Clients[2].SetLeader(test.Context, &emptypb.Empty{})
	test.Clients[1].Restore(test.Context, &emptypb.Empty{})

	test.Clients[2].SendHeartbeat(test.Context, &emptypb.Empty{})

	/*
		for idx, server := range test.Clients {
			// all should have the leaders term
			state, _ := server.GetInternalState(test.Context, &emptypb.Empty{})
			if state == nil {
				t.Fatalf("Could not get state")
			}
			fmt.Println("Server No.")
			fmt.Println(idx, state.Term)
			for idx2, ops := range state.Log {
				fmt.Println("Log No.")
				fmt.Println(idx2, ops.Term)
				fmt.Println(ops.FileMetaData.Filename)
				fmt.Println(ops.FileMetaData.Version)
			}
		}
	*/

	err = worker1.UpdateFile(file1, "update text")
	if err != nil {
		t.FailNow()
	}

	//client1 syncs
	err = SyncClient("localhost:8080", "test0", BLOCK_SIZE, cfgPath)
	if err != nil {
		t.Fatalf("Sync failed")
	}

	test.Clients[0].Restore(test.Context, &emptypb.Empty{})
	test.Clients[2].SendHeartbeat(test.Context, &emptypb.Empty{})
}
