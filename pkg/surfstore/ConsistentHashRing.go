package surfstore

import (
	"crypto/sha256"
	"encoding/hex"
	"sort"
)

type ConsistentHashRing struct {
	ServerMap map[string]string
}

func (c ConsistentHashRing) GetResponsibleServer(blockId string) string {
	//panic("todo")
	hashes := []string{}
	consistentHashRing := c.ServerMap
	for h, _ := range consistentHashRing {
		hashes = append(hashes, h)
	}
	sort.Strings(hashes)
	responsibleServer := ""
	for i := 0; i < len(hashes); i++ {
		if hashes[i] > blockId {
			responsibleServer = consistentHashRing[hashes[i]]
			break
		}
	}
	if responsibleServer == "" {
		responsibleServer = consistentHashRing[hashes[0]]
	}
	return responsibleServer
}

func (c ConsistentHashRing) Hash(addr string) string {
	h := sha256.New()
	h.Write([]byte(addr))
	return hex.EncodeToString(h.Sum(nil))

}

func NewConsistentHashRing(serverAddrs []string) *ConsistentHashRing {
	//panic("todo")
	consistentHashRing := &ConsistentHashRing{
		ServerMap: make(map[string]string),
	}
	for _, addr := range serverAddrs {
		consistentHashRing.ServerMap[consistentHashRing.Hash("blockstore"+addr)] = addr
	}
	return consistentHashRing
}
