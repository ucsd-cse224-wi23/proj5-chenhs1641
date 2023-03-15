package surfstore

import (
	context "context"

	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

type MetaStore struct {
	FileMetaMap        map[string]*FileMetaData
	BlockStoreAddrs    []string
	ConsistentHashRing *ConsistentHashRing
	UnimplementedMetaStoreServer
}

func (m *MetaStore) GetFileInfoMap(ctx context.Context, _ *emptypb.Empty) (*FileInfoMap, error) {
	// panic("todo")
	var fileInfoMap FileInfoMap
	fileInfoMap.FileInfoMap = m.FileMetaMap
	return &fileInfoMap, nil
}

func (m *MetaStore) UpdateFile(ctx context.Context, fileMetaData *FileMetaData) (*Version, error) {
	// panic("todo")
	m.FileMetaMap[fileMetaData.Filename] = fileMetaData
	return &Version{Version: fileMetaData.Version}, nil
}

func (m *MetaStore) GetBlockStoreMap(ctx context.Context, blockHashesIn *BlockHashes) (*BlockStoreMap, error) {
	// panic("todo")
	var blockStoreMap BlockStoreMap
	blockStoreMap.BlockStoreMap = make(map[string]*BlockHashes)
	for _, blockHash := range blockHashesIn.Hashes {
		server := m.ConsistentHashRing.GetResponsibleServer(blockHash)
		if blockStoreMap.BlockStoreMap[server] == nil {
			blockStoreMap.BlockStoreMap[server] = &BlockHashes{
				Hashes: make([]string, 0),
			}
		}
		blockStoreMap.BlockStoreMap[server].Hashes = append(blockStoreMap.BlockStoreMap[server].Hashes, blockHash)
	}
	return &blockStoreMap, nil
}

func (m *MetaStore) GetBlockStoreAddrs(ctx context.Context, _ *emptypb.Empty) (*BlockStoreAddrs, error) {
	// panic("todo")
	return &BlockStoreAddrs{BlockStoreAddrs: m.BlockStoreAddrs}, nil
}

// This line guarantees all method for MetaStore are implemented
var _ MetaStoreInterface = new(MetaStore)

func NewMetaStore(blockStoreAddrs []string) *MetaStore {
	return &MetaStore{
		FileMetaMap:        map[string]*FileMetaData{},
		BlockStoreAddrs:    blockStoreAddrs,
		ConsistentHashRing: NewConsistentHashRing(blockStoreAddrs),
	}
}
