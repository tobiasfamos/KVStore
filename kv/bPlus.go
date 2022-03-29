package kv

import (
	"errors"
)

type BPlusStore struct {
	rootNode   InternalNode
	bufferPool BufferPool
}

/*
	Put first element
	1. Look at root node and find ref to node we need to go.
	2. If no ref is in Yet, create the first Leaf Node: Otherwise optain Leaf node from Butterpool
	3. Insert the value into the leaf node.
	4. Mark Leaf node as dirty in Bufferpool.
	5. Return

*/

func (BPlusStore) Put(key uint64, value [10]byte) error {

	return nil
}
func (store *BPlusStore) Get(key uint64) ([10]byte, error) {
	firstChildId, _ := findPointerByKey(store.rootNode, key)
	nextPage, _ := store.bufferPool.FetchPage(PageID(firstChildId))
	nextPageNode := decodeLeafNode(nextPage.data[:])
	value, wasFound := nextPageNode.get(key)
	if !wasFound {
		return [10]byte{0}, errors.New("Could Not find Element")
	}
	return value, nil
}
func (store *BPlusStore) Create(config KvStoreConfig) error {
	//TODo Replace with better value
	newCacheEviciton := NewLRUCache(12)
	newRamDisk := NewRAMDisk(120000, 12)
	localBufferPool := NewBufferPool(12, newRamDisk, &newCacheEviciton)
	store.bufferPool = localBufferPool

	return nil

}
func (BPlusStore) Open(path string) error {
	// TODO Implement
	return nil

}
func (BPlusStore) Delete() error {
	// TODO Implement
	return nil

}
func (BPlusStore) Close() error {
	// TODO Implement
	return nil
}

// Search the partition Keys of an Internal Node to find the next Node.
// Traverse all Partition keys. As soon as a partition Key is bigger than the key to find,
// return the "previous" page ID"
func findPointerByKey(node InternalNode, key uint64) (PageID, error) {
	for index, currentKey := range node.keys {
		if currentKey > key {
			if index > 0 {
				return node.pages[index], nil
			} else {
				return node.pages[0], nil
			}
		}
	}
	return 0, errors.New("could nod find Node")
}
