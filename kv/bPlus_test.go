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

func TestGetAnElement(t *testing.T) {
	store := generateStoreWithOneElement()
	value, err := store.Get(1)
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
	newPage, _ := store.bufferPool.NewPage()
	var newData = [PageDataSize]byte{}
	copy(newData[:], dummyNode.encode())

	newPage.data = newData
	newPage.isLeaf = true
	store.bufferPool.UnpinPage(newPage.id, true)
	store.bufferPool.FlushAllPages()

	store.rootNode.keys[0] = ^uint64(0)
	store.rootNode.pages[0] = newPage.id

	return store

}
