package tmp

import (
	"errors"
	"github.com/tobiasfamos/KVStore/kv"
)

type BPlusStore struct {
	rootNode   InternalNode
	bufferPool kv.BufferPool
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
	if store.rootNode.isFull() {
		panic("Root is full, please fix")

		// // When root node is full, we must split it into two internal
		// // nodes and create a new root node.

		// leftInternal, rightInternal, separator := splitInternalNode(store.rootNode)

		// // We'll write the new left and right internal nodes to new
		// // pages, and the new root node to the previous root page.
		// leftPageId, err := createNewPage(leftInternal, &store.bufferPool)
		// if err != nil {
		// 	return err
		// }
		// rightPageId, err := createNewPage(rightInternal, &store.bufferPool)
		// if err != nil {
		// 	return err
		// }

		// // There are no pages in need of cleaning up, as the root
		// // node's page will be recycled for the new root node.

		// newRoot := InternalNode{}
		// newRoot.keys[0] = separator
		// newRoot.pages[0] = leftPageId
		// newRoot.pages[1] = rightPageId
		// newRoot.numKeys = 1
		// // TODO we do currently not actually write the root node to a
		// // page. This works as long as we will never persist it to
		// // storage, but must be implemented at latest by that time.
		// store.rootNode = newRoot

		// // After having split root node, we must actually write the value still.
		// return store.Put(key, value)
	}

	leafPage, err := findLeafPageForKey(store.rootNode, key, store.bufferPool)
	if err != nil {
		return err
	}
	leafNode := decodeLeafNode(leafPage.data[:])
	if leafNode.isFull() {
		rightLeafNode, leftLeafNode, separationKey := splitLeafNode(leafNode)
		err := writeToPageAndUnpin(leafPage, leftLeafNode, &store.bufferPool)
		if err != nil {
			return err
		}
		rightPageId, err := createNewPage(rightLeafNode, &store.bufferPool)
		if err != nil {
			return err
		}
		if store.rootNode.numKeys == 0 {
			store.rootNode.keys[0] = separationKey
			store.rootNode.pages[1] = rightPageId
			store.rootNode.numKeys += 1

			// After having split a node we still need to actually insert
			// the value which caused it to split.
			return store.Put(key, value)
		} else {
			indexToInsert := uint64(store.rootNode.numKeys)
			for keyIndex := 0; keyIndex < int(store.rootNode.numKeys); keyIndex++ {
				if store.rootNode.keys[keyIndex] > separationKey {
					indexToInsert = uint64(keyIndex)
					break
				}
			}

			// Shift the Keys to the right and insert separation key on
			//TODO Fix
			for keyIndex := uint64(store.rootNode.numKeys); keyIndex > indexToInsert; keyIndex-- {
				store.rootNode.keys[keyIndex] = store.rootNode.keys[keyIndex-1]
				store.rootNode.pages[keyIndex+1] = store.rootNode.pages[keyIndex]
			}
			store.rootNode.keys[indexToInsert] = separationKey
			store.rootNode.pages[indexToInsert+1] = rightPageId
			store.rootNode.numKeys += 1
		}

		// After having split a node we still need to actually insert
		// the value which caused it to split.
		return store.Put(key, value)
	} else {
		_, wasFound := leafNode.get(key)
		if wasFound {
			return errors.New("Key already exists")
		}
		leafNode.insert(key, value)
		writeToPageAndUnpin(leafPage, &leafNode, &store.bufferPool)
		return nil
	}
}

func splitLeafNode(leafNode LeafNode) (*LeafNode, *LeafNode, uint64) {
	middle := int(len(leafNode.values) / 2)
	leftSideKeys := leafNode.keys[:middle]
	rightSideKeys := leafNode.keys[middle:]
	leftSideValues := leafNode.values[:middle]
	rightSideValues := leafNode.values[middle:]
	rightLeafNode := new(LeafNode)
	copy(rightLeafNode.keys[:], rightSideKeys)
	copy(rightLeafNode.values[:], rightSideValues)
	rightLeafNode.numKeys = uint16(len(rightSideKeys))

	leftLeafNode := new(LeafNode)
	copy(leftLeafNode.keys[:], leftSideKeys)
	copy(leftLeafNode.values[:], leftSideValues)
	leftLeafNode.numKeys = uint16(len(leftSideKeys))

	separationKey := uint64(leftLeafNode.keys[leftLeafNode.numKeys-1])
	return rightLeafNode, leftLeafNode, separationKey
}

func splitInternalNode(node InternalNode) (*InternalNode, *InternalNode, uint64) {
	middle := len(node.pages) / 2

	leftNode := &InternalNode{}
	rightNode := &InternalNode{}

	// Initialize new left and right nodes with left and right halves of
	// splitting node's keys and values.
	copy(leftNode.keys[:], node.keys[:middle])
	copy(leftNode.pages[:], node.pages[:middle])
	leftNode.numKeys = uint16(middle) // Contains indices 0 ... mid-1

	copy(rightNode.keys[:], node.keys[middle:])
	copy(rightNode.pages[:], node.pages[middle:])
	rightNode.numKeys = uint16(len(rightNode.keys) - middle) // Contains indices mid ... len() - 1
	// Separator is chosen as the largest element of the new left node,
	// thus the "left node has keys <= separator" relation holds
	separator := uint64(leftNode.keys[leftNode.numKeys-1])

	return leftNode, rightNode, separator
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

func (store *BPlusStore) Create(config kv.KvStoreConfig) error {
	//Todo Replace with better value-
	numberOfPages := config.memorySize / kv.PageSize
	newCacheEviction := kv.NewLRUCache(20000)
	newRamDisk := kv.NewRAMDisk(config.memorySize, 20000)
	localBufferPool := kv.NewBufferPool(numberOfPages, newRamDisk, &newCacheEviction)
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
func findPointerByKey(node InternalNode, key uint64) (kv.PageID, error) {
	if node.numKeys == 0 {
		return node.pages[0], nil
	}
	for index, currentKey := range node.keys {
		if currentKey >= key || index >= int(node.numKeys) {
			if index == 0 {
				return node.pages[0], nil
			}
			if index > 0 {
				return node.pages[index], nil
			} else {
				return node.pages[node.numKeys], nil
			}
		}
	}
	return 0, errors.New("could nod find Node")
}

func findNextPageByKey(node InternalNode, key uint64, pool kv.BufferPool) (*kv.Page, error) {
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

func findLeafPageForKey(rootNode InternalNode, key uint64, pool kv.BufferPool) (*kv.Page, error) {
	var currentPage *kv.Page
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

func writeToPageAndUnpin(page *kv.Page, encondable Encoder, bufferPool *kv.BufferPool) error {
	copy(page.data[:], encondable.encode())
	err := bufferPool.UnpinPage(page.id, true)
	if err != nil {
		return err
	}
	return nil
}

func createNewPage(encodable Encoder, pool *kv.BufferPool) (kv.PageID, error) {
	newPage, err := pool.NewPage()
	if err != nil {
		return 42, err
	}
	copy(newPage.data[:], encodable.encode())
	newPage.isLeaf = true
	err = pool.UnpinPage(newPage.id, true)
	return newPage.id, nil
}