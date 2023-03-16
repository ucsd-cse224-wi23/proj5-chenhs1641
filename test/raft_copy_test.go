package SurfTest

import (
	"time"
	// "cse224/proj5/pkg/surfstore"
	"testing"

	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

func TestRaft2(t *testing.T) {
	//Setup
	cfgPath := "./config_files/3nodes.txt"
	test := InitTest(cfgPath)
	defer EndTest(test)

	// TEST
	test.Clients[0].SetLeader(test.Context, &emptypb.Empty{})
	test.Clients[0].SendHeartbeat(test.Context, &emptypb.Empty{})

	worker1 := InitDirectoryWorker("test0", SRC_PATH)
	worker2 := InitDirectoryWorker("test1", SRC_PATH)
	//defer worker1.CleanUp()
	//defer worker2.CleanUp()

	//clients add different files
	file1 := "multi_file1.txt"
	file2 := "multi_file1.txt"
	err := worker1.AddFile(file1)
	if err != nil {
		t.FailNow()
	}
	err = worker2.AddFile(file2)
	if err != nil {
		t.FailNow()
	}
	err = worker2.UpdateFile(file2, "update text")
	if err != nil {
		t.FailNow()
	}

	//client1 syncs
	err = SyncClient("localhost:8080", "test0", BLOCK_SIZE, cfgPath)
	if err != nil {
		t.Fatalf("Sync failed")
	}

	test.Clients[0].SendHeartbeat(test.Context, &emptypb.Empty{})

	test.Clients[1].Crash(test.Context, &emptypb.Empty{})
	test.Clients[2].Crash(test.Context, &emptypb.Empty{})

	test.Clients[0].SendHeartbeat(test.Context, &emptypb.Empty{})
	//client1 syncs
	go SyncClient("localhost:8080", "test1", BLOCK_SIZE, cfgPath)
	test.Clients[1].Restore(test.Context, &emptypb.Empty{})
	test.Clients[2].Restore(test.Context, &emptypb.Empty{})
	test.Clients[0].SendHeartbeat(test.Context, &emptypb.Empty{})

	time.Sleep(1 * time.Second)
	SyncClient("localhost:8080", "test0", BLOCK_SIZE, cfgPath)
	time.Sleep(2 * time.Second)
}
