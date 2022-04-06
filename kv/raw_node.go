package kv

import (
	"github.com/tobiasfamos/KVStore/search"
	"github.com/tobiasfamos/KVStore/util"
	"log"
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

func (n *INodePage) PrintDebugInfo() {
	log.Printf(
		"INode {"+
			"\n\tid:       %d"+
			"\n\tpinCount: %d"+
			"\n\tisDirty:  %t"+
			"\n\tnumKeys:  %d"+
			"\n\tkeys:     %d"+
			"\n\tpages:    %d"+
			"\n}\n",
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

func (n *LNodePage) PrintDebugInfo() {
	log.Printf(
		"LNode {"+
			"\n\tid:       %d"+
			"\n\tpinCount: %d"+
			"\n\tisDirty:  %t"+
			"\n\tnumKeys:  %d"+
			"\n\tkeys:     %d"+
			"\n\tvalues:   %d"+
			"\n}\n",
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
		log.Println("Interpreting non-INode data as INode")
		page.data[IsLeafIndex] = 0
		page.isDirty = true
	}
	numKeys := (*uint16)(unsafe.Pointer(&page.data[NumKeysIndex]))
	keys := unsafe.Slice((*uint64)(unsafe.Pointer(&page.data[KeyStartIndex])), NumInternalKeys)
	pages := unsafe.Slice((*PageID)(unsafe.Pointer(&page.data[PagesStartIndex])), NumInternalPages)

	return &INodePage{&page.id, &page.pinCount, &page.isDirty, numKeys, keys, pages}
}

// isFull returns whether the INodePage is full.
func (n *INodePage) isFull() bool {
	return *n.numKeys == NumInternalKeys
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

	idx, _ := search.Binary(key, n.keys[:*n.numKeys])
	return n.pages[idx]
}

// leftInsert inserts a new separator into an INodePage, preserving the order of keys.
// The inserted PageID will be the LEFT target node for the separator.
// Meaning that get(separator) will return the newly inserted PageID.
//
// SAFETY: The caller must ensure tree coherence in regards to separator and all child values. Otherwise the tree invariant does not hold!
//
// If the INodePage is already isFull or the node already contains the key,
// nothing will be done and the method returns false.
// Otherwise the method returns true.
func (n *INodePage) leftInsert(s uint64, id PageID) bool {
	if n.isFull() {
		return false
	}

	idx, _ := search.Binary(s, n.keys[:*n.numKeys])

	// move everything from idx onwards one up
	util.ShiftRight(n.keys, idx, uint(*n.numKeys))
	n.keys[idx] = s
	util.ShiftRight(n.pages, idx, uint(*n.numKeys)+1)
	n.pages[idx] = id
	*n.numKeys++

	*n.isDirty = true
	return true
}

/*
splitLeft splits an INodePage into two of equal size in a best effort.
This INodePage will be mutated to be the right node.

The left node is in INCONSISTENT STATE, as it has <n> keys mapped to <n> pages such that the FURTHERMOST RIGHT PageID is missing.

It returns (left, separator)
*/
func (n *INodePage) splitLeft(pageForLeftNode *Page) (*INodePage, uint64) {
	middle := *n.numKeys / 2

	left := RawINodeFrom(pageForLeftNode)
	*left.numKeys = middle
	*left.isDirty = true
	copy(left.keys[:], n.keys[:middle])
	copy(left.pages[:], n.pages[:middle])

	*n.numKeys = *n.numKeys - middle
	*n.isDirty = true
	util.ShiftLeftBy(n.keys, middle, *n.numKeys, middle)
	util.ShiftLeftBy(n.pages, middle, *n.numKeys+1, middle)

	lastValid := util.Max(0, *left.numKeys-1)
	separator := (left.keys[lastValid] + n.keys[0]) / 2

	return left, separator
}

// RawLNodeFrom explicitly transmutes a Page into an LNodePage.
// If IsLeafIndex has the wrong value it gets corrected and the page gets marked as isDirty.
func RawLNodeFrom(page *Page) *LNodePage {
	if page.data[IsLeafIndex] == 0 {
		log.Println("Interpreting non-LNode data as LNode")
		page.isDirty = true
		page.data[IsLeafIndex] = 1
	}
	numKeys := (*uint16)(unsafe.Pointer(&page.data[NumKeysIndex]))
	keys := unsafe.Slice((*uint64)(unsafe.Pointer(&page.data[KeyStartIndex])), NumLeafKeys)
	values := unsafe.Slice((*[10]byte)(unsafe.Pointer(&page.data[ValuesStartIndex])), NumLeafValues)

	return &LNodePage{&page.id, &page.pinCount, &page.isDirty, numKeys, keys, values}
}

// isFull returns whether the LNodePage is full.
func (n *LNodePage) isFull() bool {
	return *n.numKeys == NumLeafKeys
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

	util.ShiftRight(n.keys, idx, uint(*n.numKeys))
	n.keys[idx] = key

	util.ShiftRight(n.values, idx, uint(*n.numKeys))
	n.values[idx] = value

	*n.numKeys++

	*n.isDirty = true
	return true
}

/*
splitLeft splits an LNodePage into two of equal size in a best effort.
This LNodePage will be mutated to be the right node.

It returns (left, separator)
*/
func (n *LNodePage) splitLeft(pageForLeftNode *Page) (*LNodePage, uint64) {
	numKeys := *n.numKeys
	middle := numKeys / 2

	left := RawLNodeFrom(pageForLeftNode)
	*left.numKeys = middle
	*left.isDirty = true
	copy(left.keys[:], n.keys[:middle])
	copy(left.values[:], n.values[:middle])

	right := n
	*right.numKeys = numKeys - middle
	*right.isDirty = true
	util.ShiftLeftBy(right.keys, middle, numKeys, middle)
	util.ShiftLeftBy(right.values, middle, numKeys, middle)

	lastValid := util.Max(0, *left.numKeys-1)
	separator := (left.keys[lastValid] + right.keys[0]) / 2

	return left, separator
}
