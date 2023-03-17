package SurfTest

import (
	"fmt"
	"time"

	"cse224/proj5/pkg/surfstore"
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
	test.Clients[1].Crash(test.Context, &emptypb.Empty{})
	test.Clients[2].Crash(test.Context, &emptypb.Empty{})

	//client1 syncs
	in := &surfstore.FileMetaData{
		Filename:      "testFile",
		Version:       int32(1),
		BlockHashList: make([]string, 0),
	}
	go test.Clients[0].UpdateFile(test.Context, in)
	time.Sleep(1 * time.Second)
	test.Clients[1].Restore(test.Context, &emptypb.Empty{})
	test.Clients[2].Restore(test.Context, &emptypb.Empty{})
	test.Clients[0].SendHeartbeat(test.Context, &emptypb.Empty{})
	time.Sleep(1 * time.Second)
	test.Clients[0].SendHeartbeat(test.Context, &emptypb.Empty{})
	time.Sleep(1 * time.Second)
	test.Clients[0].SendHeartbeat(test.Context, &emptypb.Empty{})
	time.Sleep(1 * time.Second)

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
		fmt.Println("meta map as follows:")
		for key, value := range state.MetaMap.FileInfoMap {
			fmt.Println(key)
			fmt.Println(value)
		}
	}
}
