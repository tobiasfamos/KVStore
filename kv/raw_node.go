package kv

import (
	"fmt"
	"github.com/tobiasfamos/KVStore/search"
	"github.com/tobiasfamos/KVStore/util"
	"unsafe"
)

const (
	// IsLeafIndex is the index marking the page data to be either a LeafNode (value == 0) or an InternalNode (value != 0).
	IsLeafIndex = 0

	// NumKeysIndex is the starting index for the number of keys for both InternalNode and LeafNode.
	NumKeysIndex = 1

	// KeyStartIndex is the starting index for keys for both InternalNode and LeafNode.
	KeyStartIndex = 3

	// NumInternalKeys is the number of keys an InternalNode may hold at any given time.
	NumInternalKeys = (PageDataSize - KeyStartIndex - 4) / 12

	// NumInternalPages is the number of pages an InternalNode may hold at any given time.
	NumInternalPages = NumInternalKeys + 1

	// InternalNodeSize is the size in bytes that an InternalNode uses.
	InternalNodeSize = KeyStartIndex + 4 + 12*NumInternalKeys

	// NumLeafKeys is the number of keys a LeafNode may hold at any given time.
	NumLeafKeys = (PageDataSize - KeyStartIndex) / 18

	// NumLeafValues is the number of values a LeafNode may hold at any given time.
	NumLeafValues = NumLeafKeys

	// LeafNodeSize is the size in bytes that a LeafNode uses.
	LeafNodeSize = KeyStartIndex + 18*NumLeafKeys

	// PagesStartIndex is the starting index for the page IDs in InternalNode.
	PagesStartIndex = KeyStartIndex + NumInternalKeys*8

	// ValuesStartIndex is the starting index for the values in LeafNode.
	ValuesStartIndex = KeyStartIndex + NumLeafKeys*8
)

type KeyRange struct {
	min uint64
	max uint64
}

/*
INodePage is an internal node page that points NumInternalKeys keys to NumInternalKeys + 1 pages in a pyramid scheme.
The relationship uses less-or-equal for left-sided page IDs, greater for right-sided page IDs.

For <n = numKeys> used keys there must be <n+1> valid relations to the page IDs. Otherwise the node is corrupted.

An INodePage is a transmutation of a Page.
Any mutation on an INodePage therefore writes directly to a Page and should update the isDirty flag accordingly.
*/
type INodePage struct {
	id       *PageID
	pinCount *uint16
	isDirty  *bool
	numKeys  *uint16
	keys     []uint64
	pages    []PageID
}

func (n *INodePage) GetDebugInfo() string {
	return fmt.Sprintf(
		"INode {"+
			"\n\tid:       %d"+
			"\n\tpinCount: %d"+
			"\n\tisDirty:  %t"+
			"\n\tnumKeys:  %d"+
			"\n\tkeys:     %d"+
			"\n\tpages:    %d"+
			"\n}",
		*n.id, *n.pinCount, *n.isDirty, *n.numKeys, n.keys, n.pages,
	)
}

/*
LNodePage is a leaf node page that points NumLeafKeys keys to NumLeafKeys values in a key-value relationship.

For <n = numKeys> used keys, there are also <n> values.

An LNodePage is a transmutation of a Page.
Any mutation on an LNodePage therefore writes directly to a Page and should update the isDirty flag accordingly.
*/
type LNodePage struct {
	id       *PageID
	pinCount *uint16
	isDirty  *bool
	numKeys  *uint16
	keys     []uint64
	values   [][10]byte
}

func (n *LNodePage) GetDebugInfo() string {
	return fmt.Sprintf("LNode {"+
		"\n\tid:       %d"+
		"\n\tpinCount: %d"+
		"\n\tisDirty:  %t"+
		"\n\tnumKeys:  %d"+
		"\n\tkeys:     %d"+
		"\n\tvalues:   %d"+
		"\n}",
		*n.id, *n.pinCount, *n.isDirty, *n.numKeys, n.keys, n.values,
	)
}

// RawNodeFrom transmutes a Page into either an LNodePage or an INodePage, depending on the IsLeafIndex.
func RawNodeFrom(page *Page) (*LNodePage, *INodePage) {
	if page.data[IsLeafIndex] == 1 {
		return RawLNodeFrom(page), nil
	} else {
		return nil, RawINodeFrom(page)
	}
}

// RawINodeFrom explicitly transmutes a Page into an INodePage.
// If IsLeafIndex has the wrong value it gets corrected and the page gets marked as isDirty.
func RawINodeFrom(page *Page) *INodePage {
	if page.data[IsLeafIndex] != 0 {
		//log.Println("Interpreting non-INode data as INode")
		page.data[IsLeafIndex] = 0
		page.isDirty = true
	}
	numKeys := (*uint16)(unsafe.Pointer(&page.data[NumKeysIndex]))
	keys := unsafe.Slice((*uint64)(unsafe.Pointer(&page.data[KeyStartIndex])), NumInternalKeys)
	pages := unsafe.Slice((*PageID)(unsafe.Pointer(&page.data[PagesStartIndex])), NumInternalPages)

	return &INodePage{&page.id, &page.pinCount, &page.isDirty, numKeys, keys, pages}
}

// keyRange returns the (min, max) key range of an INodePage. If the page was empty, it returns (0, 0).
func (n *INodePage) keyRange() KeyRange {
	return KeyRange{n.keys[0], n.keys[util.Max(0, *n.numKeys-1)]}
}

// isFull returns whether the INodePage is full.
func (n *INodePage) isFull() bool {
	return *n.numKeys == NumInternalKeys
}

func (n *INodePage) isEmpty() bool {
	return *n.numKeys == 0
}

// contains returns whether the INodePage contains a specific separator.
func (n *INodePage) contains(s uint64) bool {
	_, found := search.Binary(s, n.keys[:*n.numKeys])
	return found
}

// get returns the PageID associated with a given key.
func (n *INodePage) get(key uint64) PageID {
	if *n.numKeys == 0 {
		panic("invalid state: INodePage should not be empty")
	}

	if key <= n.keys[0] {
		return n.pages[0]
	}

	for i := uint16(1); i < *n.numKeys; i++ {
		if key <= n.keys[i] {
			return n.pages[i]
		}
	}
	return n.pages[*n.numKeys]

	//idx, _ := search.Binary(key, n.keys[:*n.numKeys])
	//return n.pages[idx]
}

// rightInsert inserts a new separator into an INodePage, preserving the order of keys.
// The inserted PageID will be the RIGHT target node for the separator.
// Meaning that get(separator+1) will return the newly inserted PageID.
//
// SAFETY: The caller must ensure tree coherence in regards to separator and all child values. Otherwise the tree invariant does not hold!
//
// If the INodePage is already isFull or the node already contains the key,
// nothing will be done and the method returns false.
// Otherwise the method returns true.
func (n *INodePage) rightInsert(key uint64, id PageID) bool {
	if n.isFull() {
		return false
	}

	idx, found := search.Binary(key, n.keys[:*n.numKeys])
	if found {
		return false
	}

	// move everything from idx onwards one up
	util.ShiftRight(n.keys, idx, uint(*n.numKeys), key)
	util.ShiftRight(n.pages, idx+1, uint(*n.numKeys)+1, id)
	*n.numKeys++
	*n.isDirty = true

	return true
}

// splitRight splits an INodePage in the middle into a left (itself) and a right node.
// The right node lives in the provided page.
//
// Returns (and zeroes) the last key of the left node as a separator for the parent node (left-biased) and the right node.
func (n *INodePage) splitRight(pageForRightNode *Page) (uint64, *INodePage) {
	totalKeys := *n.numKeys
	middle := (totalKeys + 1) / 2 // ceiled as the middle gets used as parent separator

	right := RawINodeFrom(pageForRightNode)
	*right.isDirty = true
	*right.numKeys = totalKeys - middle
	util.MoveSlice(right.keys[0:*right.numKeys], n.keys[middle:totalKeys], 0)
	util.MoveSlice(right.pages[0:*right.numKeys+1], n.pages[middle:totalKeys+1], PageID(0))

	left := n
	*left.isDirty = true
	*left.numKeys = middle - 1 // offset by one as the middle will be the parent separator!

	parentSeparator := util.Replace(&left.keys[*left.numKeys], 0)

	return parentSeparator, right
}

// RawLNodeFrom explicitly transmutes a Page into an LNodePage.
// If IsLeafIndex has the wrong value it gets corrected and the page gets marked as isDirty.
func RawLNodeFrom(page *Page) *LNodePage {
	if page.data[IsLeafIndex] == 0 {
		//log.Println("Interpreting non-LNode data as LNode")
		page.isDirty = true
		page.data[IsLeafIndex] = 1
	}
	numKeys := (*uint16)(unsafe.Pointer(&page.data[NumKeysIndex]))
	keys := unsafe.Slice((*uint64)(unsafe.Pointer(&page.data[KeyStartIndex])), NumLeafKeys)
	values := unsafe.Slice((*[10]byte)(unsafe.Pointer(&page.data[ValuesStartIndex])), NumLeafValues)

	return &LNodePage{&page.id, &page.pinCount, &page.isDirty, numKeys, keys, values}
}

// keyRange returns the (min, max) key range of an INodePage. If the page was empty, it returns (0, 0).
func (n *LNodePage) keyRange() KeyRange {
	return KeyRange{n.keys[0], n.keys[util.Max(0, *n.numKeys-1)]}
}

// isFull returns whether the LNodePage is full.
func (n *LNodePage) isFull() bool {
	return *n.numKeys == NumLeafKeys
}

func (n *LNodePage) isEmpty() bool {
	return *n.numKeys == 0
}

// contains returns whether an LNodePage contain a specific key.
func (n *LNodePage) contains(key uint64) bool {
	_, found := search.Binary(key, n.keys[:*n.numKeys])
	return found
}

// get returns the PageID associated with a given key.
// If no association was found, the second return value is false.
func (n *LNodePage) get(key uint64) ([10]byte, bool) {
	idx, found := search.Binary(key, n.keys[:*n.numKeys])
	if found {
		return n.values[idx], true
	} else {
		return [10]byte{}, false
	}
}

/*
insert() inserts a key with a given value into an LNodePage.
If the LNodePage is already isFull or the node already contains the key,
nothing will be done and the method returns false.
Otherwise, the key-value pair will be inserted, preserving the order inside the LNodePage,
and the method returns true.
*/
func (n *LNodePage) insert(key uint64, value [10]byte) bool {
	if n.isFull() {
		return false
	}

	idx, found := search.Binary(key, n.keys[:*n.numKeys])
	if found {
		return false
	}

	util.ShiftRight(n.keys, idx, uint(*n.numKeys), key)
	util.ShiftRight(n.values, idx, uint(*n.numKeys), value)

	*n.numKeys++

	*n.isDirty = true
	return true
}

// splitRight splits an LNodePage in the middle into a left (itself) and a right node.
// The right node lives in the provided page.
//
// Returns the last key of the left node as a separator for the parent node (left-biased) and the right node.
func (n *LNodePage) splitRight(pageForRightNode *Page) (uint64, *LNodePage) {
	totalKeys := *n.numKeys
	middle := totalKeys / 2 // floored

	right := RawLNodeFrom(pageForRightNode)
	*right.isDirty = true
	*right.numKeys = totalKeys - middle
	util.MoveSlice(right.keys[0:*right.numKeys], n.keys[middle:totalKeys], 0)
	util.MoveSlice(right.values[0:*right.numKeys], n.values[middle:totalKeys], [10]byte{})

	left := n
	*left.isDirty = true
	*left.numKeys = middle

	parentSeparator := left.keys[*left.numKeys-1]

	return parentSeparator, right
}
