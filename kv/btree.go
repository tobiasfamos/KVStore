package kv

import (
	"errors"
	"math"
)

type BTree struct {
	bufferPool BufferPool
	root       InternalNode
	rootPage   *Page
}

func (t *BTree) Create(config KvStoreConfig) error {
	numberOfPages := config.memorySize / PageSize
	newCacheEviction := NewLRUCache(20000)
	newRamDisk := NewRAMDisk(config.memorySize, 20000)
	t.bufferPool = NewBufferPool(numberOfPages, newRamDisk, &newCacheEviction)

	var err error
	t.rootPage, err = t.bufferPool.NewPage()
	if err != nil {
		return err
	}

	leftPage, err := t.bufferPool.NewPage()
	rightPage, err := t.bufferPool.NewPage()
	err = t.bufferPool.UnpinPage(leftPage.id, true)
	if err != nil {
		return err
	}
	err = t.bufferPool.UnpinPage(rightPage.id, true)
	if err != nil {
		return err
	}

	t.root = InternalNode{}
	t.root.numKeys = 1
	t.root.keys[0] = math.MaxUint64 / 2
	t.root.pages[0] = leftPage.id
	t.root.pages[1] = rightPage.id

	copy(t.rootPage.data[:], t.root.encode())
	err = t.bufferPool.UnpinPage(t.rootPage.id, false)
	if err != nil {
		return err
	}

	return nil
}

func (t *BTree) Open(path string) error {
	//TODO implement me
	panic("implement me")
}

func (t *BTree) Delete() error {
	//TODO implement me
	panic("implement me")
}

func (t *BTree) Close() error {
	//TODO implement me
	panic("implement me")
}

type visitor struct {
	page *Page
	node *InternalNode
}
type leafer struct {
	page *Page
	node LeafNode
}

func (t *BTree) Get(key uint64) ([10]byte, error) {
	_, leaf := t.traverseTo(key)
	value, found := leaf.node.get(key)

	if !found {
		return value, errors.New("value not found")
	} else {
		return value, nil
	}
}

func (t *BTree) Put(key uint64, value [10]byte) error {
	bufPool := t.bufferPool

	visited, leaf := t.traverseTo(key)

	if !leaf.node.isFull() {
		leaf.node.insert(key, value)

		// Cleanup
		for _, internal := range visited {
			err := bufPool.UnpinPage(internal.page.id, false)
			if err != nil {
				return err
			}
		}
		err := bufPool.UnpinPage(leaf.page.id, true)
		if err != nil {
			return err
		}
	} else {
		t.splitLeaf(visited, leaf, key, value)
	}

	return nil
}

func (t *BTree) traverseTo(key uint64) ([]visitor, leafer) {
	visited := []visitor{{t.rootPage, &t.root}}
	var leaf leafer

	// find leaf
	for leaf.page == nil {
		id := visited[len(visited)-1].node.getPage(key)
		page, _ := t.bufferPool.FetchPage(id)

		if page.isLeaf {
			leaf.page = page
			leaf.node = decodeLeafNode(page.data[:])
		} else {
			iNode := decodeInternalNode(page.data[:])
			visited = append(visited, visitor{page, &iNode})
		}
	}

	return visited, leaf
}

func (t *BTree) splitLeaf(visited []visitor, leaf leafer, key uint64, value [10]byte) {
	left, right, separator := leaf.node.split()

	left.insert(key, value)
	leftPage, _ := t.bufferPool.NewPage()
	copy(leftPage.data[:], left.encode())
	_ = t.bufferPool.UnpinPage(leftPage.id, true)

	copy(leaf.page.data[:], right.encode())
	_ = t.bufferPool.UnpinPage(leaf.page.id, true)

	t.insertToParent(visited, separator, leftPage.id)
}

func (t *BTree) insertToParent(visited []visitor, separator uint64, pageID PageID) {
	if len(visited) == 0 {
		t.insertNewRoot(separator, pageID)
		return
	}

	last := len(visited) - 1
	parent := visited[last]

	if !parent.node.isFull() {
		parent.node.leftInsertPage(separator, pageID)

		// Cleanup
		for _, internal := range visited {
			isDirty := parent == internal
			_ = t.bufferPool.UnpinPage(internal.page.id, isDirty)
		}
	} else {
		t.splitInternal(visited[:last], parent)
	}
}

func (t *BTree) splitInternal(visited []visitor, child visitor) {
	left, right, separator := child.node.split()

	leftPage, _ := t.bufferPool.NewPage()
	copy(leftPage.data[:], left.encode())
	_ = t.bufferPool.UnpinPage(leftPage.id, true)

	copy(child.page.data[:], right.encode())
	_ = t.bufferPool.UnpinPage(child.page.id, true)

	t.insertToParent(visited, separator, leftPage.id)
}

func (t *BTree) insertNewRoot(separator uint64, id PageID) {
	panic("unimplemented")
}
