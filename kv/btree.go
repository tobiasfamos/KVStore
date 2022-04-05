package kv

import (
	"errors"
	"math"
)

type BTree struct {
	bufferPool BufferPool
	root       *INodePage
	rootPage   *Page
}

func (t *BTree) createInitialTree() error {
	var err error
	t.rootPage, err = t.bufferPool.NewPage()
	if err != nil {
		return err
	}

	leftPage, err := t.bufferPool.NewPage()
	if err != nil {
		return err
	}
	_ = RawLNodeFrom(leftPage) // automatically sets isLeaf flag
	err = t.bufferPool.UnpinPage(leftPage.id, true)
	if err != nil {
		return err
	}

	rightPage, err := t.bufferPool.NewPage()
	if err != nil {
		return err
	}
	_ = RawLNodeFrom(rightPage) // automatically sets isLeaf flag
	err = t.bufferPool.UnpinPage(rightPage.id, true)
	if err != nil {
		return err
	}

	t.root = RawINodeFrom(t.rootPage)
	*t.root.numKeys = 1
	t.root.keys[0] = math.MaxUint64 / 2
	t.root.pages[0] = leftPage.id
	t.root.pages[1] = rightPage.id
	return t.bufferPool.UnpinPage(t.rootPage.id, true)
}

func (t *BTree) Create(config KvStoreConfig) error {
	numberOfPages := config.memorySize / PageSize
	newCacheEviction := NewLRUCache(20000)
	newRamDisk := NewRAMDisk(config.memorySize, 20000)
	t.bufferPool = NewBufferPool(numberOfPages, newRamDisk, &newCacheEviction)

	if err := t.createInitialTree(); err != nil {
		return nil
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

func (t *BTree) Get(key uint64) ([10]byte, error) {
	lastNode := t.root
	var leaf *LNodePage

	// find leaf
	for leaf == nil {
		id := lastNode.get(key)
		page, err := t.bufferPool.FetchPage(id)
		if err != nil {
			return [10]byte{}, err
		}

		l, i := RawNodeFrom(page)
		if l != nil {
			leaf = l
		} else {
			lastNode = i
		}
	}

	value, found := leaf.get(key)
	if !found {
		return value, errors.New("value not found")
	} else {
		return value, nil
	}
}

func (t *BTree) Put(key uint64, value [10]byte) error {
	visited, leaf, err := t.traverseTo(key)
	if err != nil {
		return err
	}

	if !leaf.isFull() {
		leaf.insert(key, value)

		// Cleanup
		for _, internal := range visited {
			err = t.bufferPool.UnpinPage(*internal.id, false)
			if err != nil {
				return err
			}
		}
		return t.bufferPool.UnpinPage(*leaf.id, true)
	} else {
		return t.splitLeaf(visited, leaf, key, value)
	}
}

func (t *BTree) traverseTo(key uint64) ([]*INodePage, *LNodePage, error) {
	visited := []*INodePage{t.root}
	var leaf *LNodePage

	// find leaf
	for leaf == nil {
		id := visited[len(visited)-1].get(key)
		page, err := t.bufferPool.FetchPage(id)
		if err != nil {
			return nil, nil, err
		}

		l, i := RawNodeFrom(page)
		if l != nil {
			leaf = l
		} else {
			visited = append(visited, i)
		}
	}

	return visited, leaf, nil
}

func (t *BTree) splitLeaf(visited []*INodePage, leaf *LNodePage, key uint64, value [10]byte) error {
	leftPage, err := t.bufferPool.NewPage()
	if err != nil {
		return err
	}
	left, separator := leaf.splitLeft(leftPage)
	// leaf is the new right node
	right := leaf

	if key <= separator {
		left.insert(key, value)
	} else {
		right.insert(key, value)
	}

	err = t.bufferPool.UnpinPage(*left.id, true)
	if err != nil {
		return err
	}
	err = t.bufferPool.UnpinPage(*right.id, true)
	if err != nil {
		return err
	}

	return t.insertToParent(visited, separator, leftPage.id)
}

func (t *BTree) insertToParent(visited []*INodePage, separator uint64, pageID PageID) error {
	if pageID == PageID(0) {
		t.insertNewRoot(separator, pageID)
		return nil
	}

	last := len(visited) - 1
	parent := visited[last]

	if !parent.isFull() {
		parent.leftInsert(separator, pageID)

		// Cleanup
		for _, internal := range visited {
			_ = t.bufferPool.UnpinPage(*internal.id, true)
		}
	} else {
		return t.splitInternal(visited[:last], parent)
	}

	return nil
}

func (t *BTree) splitInternal(visited []*INodePage, child *INodePage) error {
	leftPage, err := t.bufferPool.NewPage()
	if err != nil {
		return err
	}

	left, separator := child.splitLeft(leftPage)
	// child is the new right node
	right := child

	err = t.bufferPool.UnpinPage(*left.id, true)
	if err != nil {
		return err
	}
	err = t.bufferPool.UnpinPage(*right.id, true)
	if err != nil {
		return err
	}

	return t.insertToParent(visited, separator, *left.id)
}

func (t *BTree) insertNewRoot(separator uint64, id PageID) {
	panic("unimplemented")
}
