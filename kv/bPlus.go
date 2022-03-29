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

func (store *BPlusStore) Put(key uint64, value [10]byte) error {
	leafPage, err := findLeafPageForKey(store.rootNode, key, store.bufferPool)
	if err != nil {
		return err
	}
	leafNode := decodeLeafNode(leafPage.data[:])
	_, wasFound := leafNode.get(key)
	if wasFound {
		return errors.New("Key already exists")
	}
	leafNode.insert(key, value)
	writeLeafToPageAndUnpin(leafPage, &leafNode, &store.bufferPool)
	return nil
}
func (store *BPlusStore) Get(key uint64) ([10]byte, error) {
	leafPage, err := findLeafPageForKey(store.rootNode, key, store.bufferPool)
	if err != nil {
		return [10]byte{}, err
	}
	leafNode := decodeLeafNode(leafPage.data[:])
	value, wasFound := leafNode.get(key)
	if !wasFound {
		return [10]byte{0}, errors.New("Could Not find Element")
	}
	return value, nil
}

func (store *BPlusStore) Create(config KvStoreConfig) error {
	//TODo Replace with better value
	numberOfPages := config.memorySize / PageSize
	newCacheEviction := NewLRUCache(12)
	newRamDisk := NewRAMDisk(uint32(config.memorySize), 12)
	localBufferPool := NewBufferPool(numberOfPages, newRamDisk, &newCacheEviction)
	store.bufferPool = localBufferPool
	newPage, err := store.bufferPool.NewPage()
	if err != nil {
		return err
	}
	copy(newPage.data[:], new(LeafNode).encode())
	newPage.isLeaf = true
	store.rootNode.pages[0] = newPage.id
	err = store.bufferPool.UnpinPage(newPage.id, true)
	if err != nil {
		return err
	}
	return nil

}
func (store *BPlusStore) Open(path string) error {
	// TODO Implement
	return nil

}
func (store *BPlusStore) Delete() error {
	// TODO Implement
	return nil

}
func (store *BPlusStore) Close() error {
	// TODO Implement
	return nil
}

// Search the partition Keys of an Internal Node to find the next Node.
// Traverse all Partition keys. As soon as a partition Key is bigger than the key to find,
// return the "previous" page ID"
func findPointerByKey(node InternalNode, key uint64) (PageID, error) {
	if node.numKeys == 0 {
		return node.pages[0], nil
	}
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

func findNextPageByKey(node InternalNode, key uint64, pool BufferPool) (*Page, error) {
	nextNodeID, err := findPointerByKey(node, key)
	if err != nil {
		return nil, err
	}
	nextPage, err := pool.FetchPage(nextNodeID)
	if err != nil {
		return nil, err
	}
	return nextPage, nil
}

func findLeafPageForKey(rootNode InternalNode, key uint64, pool BufferPool) (*Page, error) {
	var currentPage *Page
	currentNode := rootNode
	for true {
		var err error
		currentPage, err = findNextPageByKey(currentNode, key, pool)
		if err != nil {
			return nil, err
		}
		if currentPage.isLeaf {
			break
		}
		currentNode = decodeInternalNode(currentPage.data[:])
	}
	return currentPage, nil

}

func writeLeafToPageAndUnpin(page *Page, leafNode *LeafNode, bufferPool *BufferPool) error {
	copy(page.data[:], leafNode.encode())
	err := bufferPool.UnpinPage(page.id, true)
	if err != nil {
		return err
	}
	return nil
}
