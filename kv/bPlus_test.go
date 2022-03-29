package kv

import (
	"fmt"
	"testing"
)

func emptyStore() *BPlusStore {
	return NewBPlusStore()
}

func TestInitialize(t *testing.T) {
	store := emptyStore()
	fmt.Print(store)
}

func TestPutFirstElement(t *testing.T) {
	store := emptyStore()
	store.rootNode.keys[0] = 0
	store.rootNode.keys[1] = 10
	store.rootNode.keys[2] = 20
	store.rootNode.keys[3] = 30

	store.rootNode.pages[0] = 0
	store.rootNode.pages[1] = 1
	store.rootNode.pages[2] = 2
	store.rootNode.pages[3] = 3
	store.rootNode.pages[4] = 4

	err := store.Put(12, [10]byte{10, 10, 1})
	if err != nil {
		t.Errorf("Could not Put")
	}
}

func TestGetAnElement(t *testing.T) {
	store := emptyStore()
	value, err := store.Get(12)
	if err != nil {
		t.Errorf("Could not Get")
	}
	if value != [10]byte{2} {
		t.Errorf("Value is not 2")
	}
}

//Create Store with key: 1 and value 2
func generateStoreWithOneElement() *BPlusStore {

	store := emptyStore()
	dummyNode := LeafNode{
		keys:    [227]uint64{1},
		values:  [227][10]byte{{2}},
		numKeys: 1,
	}
	newPage := store.bufferPool.NewPage()
	var newData = [PageDataSize]byte{}
	copy(newData[:], dummyNode.encode())

	newPage.data = newData
	newPage.isDirty = true
	newPage.isLeaf = true
	store.bufferPool.UnpinPage(newPage.id, true)
	store.bufferPool.FlushAllPages()

	store.rootNode.keys[0] = ^uint64(0)
	store.rootNode.pages[0] = uint32(newPage.id)

	return store

}
