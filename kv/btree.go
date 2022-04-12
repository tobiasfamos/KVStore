package kv

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
)

// treeMetaDataFile specifies the name of the file used by the tree to store
// its meta data.
const treeMetaDataFile = "tree.meta"

type BTree struct {
	bufferPool BufferPool
	root       *INodePage
	rootPage   *Page

	// root directory where tree is persisted to. Will be empty in case of
	// a memory store.
	directory string

	// Whether the tree can be read from. If set to false, all read/write
	// operations will panic.
	open bool
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

	rightPage, err := t.bufferPool.NewPage()
	if err != nil {
		return err
	}
	_ = RawLNodeFrom(rightPage) // automatically sets isLeaf flag

	t.root = RawINodeFrom(t.rootPage)
	*t.root.isDirty = true
	*t.root.numKeys = 1
	t.root.keys[0] = math.MaxUint64 / 2
	t.root.pages[0] = leftPage.id
	t.root.pages[1] = rightPage.id

	// Cleanup
	t.bufferPool.UnpinPage(leftPage.id, true)
	t.bufferPool.UnpinPage(rightPage.id, true)

	return nil
}

func (t *BTree) loadExistingTree() error {
	var err error

	rootPageID, err := t.loadMetaData()
	if err != nil {
		return err
	}

	t.rootPage, err = t.bufferPool.FetchPage(rootPageID)
	if err != nil {
		return err
	}

	leftPage, err := t.bufferPool.NewPage()
	if err != nil {
		return err
	}
	_ = RawLNodeFrom(leftPage) // automatically sets isLeaf flag

	rightPage, err := t.bufferPool.NewPage()
	if err != nil {
		return err
	}
	_ = RawLNodeFrom(rightPage) // automatically sets isLeaf flag

	t.root = RawINodeFrom(t.rootPage)
	*t.root.isDirty = false // We just read it from disk

	return nil
}

func (t *BTree) Create(config KvStoreConfig) error {
	numberOfPages := config.memorySize / PageSize
	newCacheEviction := NewLRUCache(numberOfPages)

	t.directory = config.workingDirectory

	persistentDisk, err := NewPersistentDisk(config.workingDirectory)
	if err != nil {
		return err
	}

	t.bufferPool = NewBufferPool(numberOfPages, persistentDisk, &newCacheEviction)

	if err := t.createInitialTree(); err != nil {
		return nil // FIXME: Why do we swall the error?
	}

	// Tree initialized successfully
	t.directory = config.workingDirectory
	t.open = true

	return nil
}

func (t *BTree) Open(config KvStoreConfig) error {
	numberOfPages := config.memorySize / PageSize
	newCacheEviction := NewLRUCache(numberOfPages)

	persistentDisk, err := NewPersistentDisk(config.workingDirectory)
	if err != nil {
		return err
	}

	t.bufferPool = NewBufferPool(numberOfPages, persistentDisk, &newCacheEviction)

	// Must be set before we load the tree, as it's used by it
	t.directory = config.workingDirectory

	if err := t.loadExistingTree(); err != nil {
		return err
	}

	// Tree loaded successfully
	t.open = true
	return nil
}

func (t *BTree) Delete() error {
	if !t.open {
		panic("Cannot delete closed tree")
	}

	err := os.RemoveAll(t.directory)
	if err != nil {
		return fmt.Errorf("IO error while deleting store directory: %v", err)
	}

	t.open = false

	return nil
}

func (t *BTree) Close() error {
	if !t.open {
		panic("Cannot close closed tree")
	}

	err := t.bufferPool.Close()
	if err != nil {
		return fmt.Errorf("Error closing buffer pool: %v", err)
	}

	// Now we'll only need to persist our own meta data, and we're golden.
	return t.storeMetaData()
}

func (t *BTree) loadMetaData() (rootPageID PageID, err error) {
	data := make([]byte, 4)

	metaFilePath := filepath.Join(t.directory, treeMetaDataFile)
	file, err := os.Open(metaFilePath)
	if err != nil {
		return rootPageID, fmt.Errorf("IO error while opening tree meta data file: %v", err)
	}
	defer file.Close()

	_, err = file.ReadAt(data, 0)
	if err != nil {
		return rootPageID, fmt.Errorf("IO error while reading tree meta data file: %v", err)
	}

	rootPageID = PageID(binary.BigEndian.Uint32(data))

	return rootPageID, nil
}

func (t *BTree) storeMetaData() error {
	data := make([]byte, 4)
	// Root page ID
	binary.BigEndian.PutUint32(data[0:4], uint32(t.rootPage.id))

	metaFilePath := filepath.Join(t.directory, treeMetaDataFile)
	// We can simply truncate it
	file, err := os.Create(metaFilePath)
	if err != nil {
		return fmt.Errorf("IO error while opening tree meta data file: %v", err)
	}
	defer file.Close()

	_, err = file.Write(data)
	if err != nil {
		return fmt.Errorf("IO error while writing tree meta data: %v", err)
	}

	return nil
}

func (t *BTree) GetDebugInformation() string {
	return fmt.Sprintf("%T {"+
		"\n\troot:\n%s"+
		"\n\tbufferPool:\n%s"+
		"}",
		t, t.root.GetDebugInfo(), t.bufferPool.GetDebugInfo(),
	)
}

var printed = false

func (t *BTree) Get(key uint64) ([10]byte, error) {
	if !t.open {
		panic("Cannot read from closed tree")
	}

	lastNode := t.root
	var leaf *LNodePage

	trace := fmt.Sprint(*lastNode.id, " -> ")
	// find leaf
	for leaf == nil {
		id := lastNode.get(key)
		page, err := t.bufferPool.FetchPage(id)
		if err != nil {
			return [10]byte{}, err
		}

		l, i := RawNodeFrom(page)
		if l != nil {
			trace += fmt.Sprint(*l.id)
			leaf = l
		} else {
			trace += fmt.Sprint(*i.id, " -> ")
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
	if !t.open {
		panic("Cannot write to closed tree")
	}

	trace, leaf, err := t.traceTo(key)
	if err != nil {
		return err
	}

	if leaf.isFull() {
		return t.splitLeaf(trace, leaf, key, value)
	} else {
		if !leaf.insert(key, value) {
			return errors.New("unable to re-insert existing key")
		}

		// Cleanup
		for _, internal := range trace {
			if *internal.id != *t.root.id {
				t.bufferPool.UnpinPage(*internal.id, false)
			}
		}
		t.bufferPool.UnpinPage(*leaf.id, true)
	}

	return nil
}

func (t *BTree) traceTo(key uint64) ([]*INodePage, *LNodePage, error) {
	trace := []*INodePage{t.root}
	var leaf *LNodePage

	// find leaf
	for leaf == nil {
		id := trace[len(trace)-1].get(key)
		page, err := t.bufferPool.FetchPage(id)
		if err != nil {
			return nil, nil, err
		}

		l, i := RawNodeFrom(page)
		if l != nil {
			leaf = l
		} else {
			trace = append(trace, i)
		}
	}

	return trace, leaf, nil
}

func (t *BTree) splitLeaf(trace []*INodePage, leaf *LNodePage, key uint64, value [10]byte) error {
	// create new page for right node, current leaf will be reused for left node.
	rightPage, err := t.bufferPool.NewPage()
	if err != nil {
		return err
	}

	separator, right := leaf.splitRight(rightPage)
	// leaf is the new left node.
	left := leaf

	// insert key accordingly
	if key <= separator {
		left.insert(key, value)
	} else {
		right.insert(key, value)
	}

	newRightID := *right.id

	// Cleanup
	t.bufferPool.UnpinPage(*left.id, true)
	t.bufferPool.UnpinPage(*right.id, true)

	// insert right node to parent.
	return t.insertToParent(trace, separator, newRightID)
}

func (t *BTree) insertToParent(trace []*INodePage, separator uint64, newRight PageID) error {
	last := len(trace) - 1
	parent := trace[last]

	if !parent.isFull() {
		parent.rightInsert(separator, newRight)
		// Cleanup
		for _, internal := range trace {
			if *internal.id != *t.root.id {
				t.bufferPool.UnpinPage(*internal.id, true)
			}
		}
	} else {
		left, right, err := t.splitInternal(trace[:last], parent)
		if err != nil {
			return err
		}

		// insert left if right node has higher values and left node has less-or-equal number of keys.
		if min, _ := right.keyRange(); min <= separator && *left.numKeys <= *right.numKeys {
			left.rightInsert(separator, newRight)
			t.bufferPool.UnpinPage(*left.id, true)   // override left as dirty
			t.bufferPool.UnpinPage(*right.id, false) // may still be dirty
		} else {
			right.rightInsert(separator, newRight)
			t.bufferPool.UnpinPage(*left.id, false)
			t.bufferPool.UnpinPage(*right.id, true)
		}
	}

	return nil
}

func (t *BTree) splitInternal(trace []*INodePage, splittingNode *INodePage) (*INodePage, *INodePage, error) {
	rightPage, err := t.bufferPool.NewPage()
	if err != nil {
		return nil, nil, err
	}

	separator, right := splittingNode.splitRight(rightPage)
	left := splittingNode

	// create new root if needed and make it the trace
	if *splittingNode.id == *t.root.id {
		if len(trace) != 0 {
			panic("DEV: logic error")
		}
		if err = t.createNewRoot(separator, *left.id, *right.id); err != nil {
			return nil, nil, err
		}
		trace = []*INodePage{t.root}
	}

	// add the split to the parent
	if err = t.insertToParent(trace, separator, *right.id); err != nil {
		return nil, nil, err
	}

	return left, right, nil
}

func (t *BTree) createNewRoot(separator uint64, leftID PageID, rightID PageID) error {
	newRootPage, err := t.bufferPool.NewPage()
	if err != nil {
		return err
	}

	newRoot := RawINodeFrom(newRootPage)
	*newRoot.isDirty = true
	*newRoot.numKeys = 1
	newRoot.keys[0] = separator
	newRoot.pages[0] = leftID
	newRoot.pages[1] = rightID

	t.bufferPool.UnpinPage(t.rootPage.id, false)

	t.root = newRoot
	t.rootPage = newRootPage

	return nil
}

//func (t *BTree) splitInternal(visited []*INodePage, splittingNode *INodePage) (*INodePage, *INodePage, error) {
//	leftPage, err := t.bufferPool.NewPage()
//	if err != nil {
//		return nil, nil, err
//	}
//
//	left, separator := splittingNode.splitLeft(leftPage)
//	// splittingNode is the new right node
//	right := splittingNode
//
//	leftID := *left.id
//	rightID := *right.id
//
//	t.bufferPool.UnpinPage(*left.id, true)
//	t.bufferPool.UnpinPage(*right.id, true)
//
//	if splittingNode == t.root {
//		return t.newRootWith(leftID, rightID, separator)
//	}
//
//	return t.insertToParent(visited, separator, leftID)
//}
