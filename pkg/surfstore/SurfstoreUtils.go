package surfstore

import (
	"os"
	"reflect"
)

func contains(s []string, e string) bool {
	for _, a := range s {
		if a == e {
			return true
		}
	}
	return false
}

// Implement the logic for a client syncing with the server here.
func ClientSync(client RPCClient) {
	// panic("todo")
	indexdbPath := ConcatPath(client.BaseDir, DEFAULT_META_FILENAME)
	_, err := os.Stat(indexdbPath)
	if err != nil {
		if os.IsNotExist(err) {
			_, err = os.Create(indexdbPath)
			if err != nil {
				panic(err)
			}
		} else {
			panic(err)
		}
	}

	files, err := os.ReadDir(client.BaseDir)
	if err != nil {
		panic(err)
	}

	localIndex, err := LoadMetaFromMetaFile(client.BaseDir)
	if err != nil {
		panic(err)
	}

	for _, file := range files {
		if file.Name() != DEFAULT_META_FILENAME {
			fileInfo, err := file.Info()
			if err != nil {
				panic(err)
			}
			numOfBlocks := int(fileInfo.Size()) / client.BlockSize
			if int(fileInfo.Size())%client.BlockSize > 0 {
				numOfBlocks++
			}
			openFile, err := os.Open(ConcatPath(client.BaseDir, file.Name()))
			if err != nil {
				panic(err)
			}
			if _, ok := localIndex[file.Name()]; !ok {
				localIndex[file.Name()] = &FileMetaData{
					Filename:      file.Name(),
					Version:       0,
					BlockHashList: make([]string, 0), // modify to empty init
				}
			}
			if numOfBlocks == 0 {
				if !(len(localIndex[file.Name()].BlockHashList) == 1 && localIndex[file.Name()].BlockHashList[0] == "-1") {
					localIndex[file.Name()].Version++
					localIndex[file.Name()].BlockHashList = make([]string, 1)
					localIndex[file.Name()].BlockHashList[0] = "-1"
				}
			} else {
				if len(localIndex[file.Name()].BlockHashList) > numOfBlocks {
					localIndex[file.Name()].BlockHashList = localIndex[file.Name()].BlockHashList[:numOfBlocks]
				}
				modified := false
				for i := 0; i < numOfBlocks; i++ {
					buffer := make([]byte, client.BlockSize)
					length, err := openFile.Read(buffer)
					if err != nil {
						panic(err)
					}
					buffer = buffer[:length]
					blockHashString := GetBlockHashString(buffer)
					if i >= len(localIndex[file.Name()].BlockHashList) {
						localIndex[file.Name()].BlockHashList = append(localIndex[file.Name()].BlockHashList, blockHashString)
						modified = true
					} else {
						if localIndex[file.Name()].BlockHashList[i] != blockHashString {
							localIndex[file.Name()].BlockHashList[i] = blockHashString
							modified = true
						}
					}
				}
				if modified {
					localIndex[file.Name()].Version++
				}
			}
			openFile.Close()
		}
	}

	for localFileName, localFileMetaData := range localIndex {
		filePath := ConcatPath(client.BaseDir, localFileName)
		if _, err := os.Stat(filePath); os.IsNotExist(err) {
			if !(len(localFileMetaData.BlockHashList) == 1 && localFileMetaData.BlockHashList[0] == "0") {
				localFileMetaData.Version++
				localFileMetaData.BlockHashList = make([]string, 1)
				localFileMetaData.BlockHashList[0] = "0"
			}
		}
	}
	// PrintMetaMap(localIndex)
	// below: sync with server

	var blockStoreAddrs []string
	if err := client.GetBlockStoreAddrs(&blockStoreAddrs); err != nil {
		panic(err)
	}

	remoteIndex := make(map[string]*FileMetaData)
	if err := client.GetFileInfoMap(&remoteIndex); err != nil {
		panic(err)
	}
	if len(remoteIndex) == 0 {
		remoteIndex = make(map[string]*FileMetaData)
	}

	for remoteFileName, remoteFileMetaData := range remoteIndex {
		if localFileMetaData, ok := localIndex[remoteFileName]; ok {
			if localFileMetaData.Version > remoteFileMetaData.Version {
				continue // the file on server is old, should not download
			} else if localFileMetaData.Version == remoteFileMetaData.Version && reflect.DeepEqual(localFileMetaData.BlockHashList, remoteFileMetaData.BlockHashList) {
				continue
			}
		}
		localIndex[remoteFileName] = remoteFileMetaData

		filePath := ConcatPath(client.BaseDir, remoteFileName)

		if len(remoteFileMetaData.BlockHashList) == 1 && remoteFileMetaData.BlockHashList[0] == "0" { // removed from server, should delete
			if err := os.RemoveAll(filePath); err != nil {
				panic(err)
			}
		} else { // write to local file
			file, err := os.Create(filePath)
			if err != nil {
				panic(err)
			}
			var blockStoreMap map[string][]string
			if err := client.GetBlockStoreMap(remoteFileMetaData.BlockHashList, &blockStoreMap); err != nil {
				panic(err)
			}
			for _, blockHash := range remoteFileMetaData.BlockHashList {
				var block Block
				var blockStoreAddr string
				for addr, hashes := range blockStoreMap {
					if contains(hashes, blockHash) {
						blockStoreAddr = addr
						break
					}
				}
				if err := client.GetBlock(blockHash, blockStoreAddr, &block); err != nil {
					panic(err)
				}
				file.Write(block.BlockData)
			}
			file.Close()
		}
	}
	for localFileName, localFileMetaData := range localIndex {
		if remoteFileMetaData, ok := remoteIndex[localFileName]; ok {
			if localFileMetaData.Version <= remoteFileMetaData.Version {
				continue // the file on server is new enough, should not upload
			}
		}
		remoteIndex[localFileName] = localFileMetaData
		var latestVersion int32
		if err := client.UpdateFile(localFileMetaData, &latestVersion); err != nil {
			panic(err)
		}
		localFileMetaData.Version = latestVersion
		if !(len(localFileMetaData.BlockHashList) == 1 && localFileMetaData.BlockHashList[0] == "0") { // exist file, should have sth sent to server
			// read from local file
			filePath := ConcatPath(client.BaseDir, localFileName)
			file, err := os.Open(filePath)
			if err != nil {
				panic(err)
			}
			fileInfo, err := os.Stat(filePath)
			if err != nil {
				panic(err)
			}
			numOfBlocks := int(fileInfo.Size()) / client.BlockSize
			if int(fileInfo.Size())%client.BlockSize > 0 {
				numOfBlocks++
			}
			var blockStoreMap map[string][]string
			if err := client.GetBlockStoreMap(localFileMetaData.BlockHashList, &blockStoreMap); err != nil {
				panic(err)
			}
			for i := 0; i < numOfBlocks; i++ {
				buffer := make([]byte, client.BlockSize)
				length, err := file.Read(buffer)
				if err != nil {
					panic(err)
				}
				buffer = buffer[:length]
				var succ bool
				var blockStoreAddr string
				for addr, hashes := range blockStoreMap {
					if contains(hashes, localFileMetaData.BlockHashList[i]) {
						blockStoreAddr = addr
						break
					}
				}
				if err := client.PutBlock(&Block{
					BlockData: buffer,
					BlockSize: int32(length),
				}, blockStoreAddr, &succ); err != nil {
					panic(err)
				}
			}
			file.Close()
		}
	}
	// below: write to local db
	// PrintMetaMap(remoteIndex)
	err = WriteMetaFile(localIndex, client.BaseDir)
	if err != nil {
		panic(err)
	}
}
