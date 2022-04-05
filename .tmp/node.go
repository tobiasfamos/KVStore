package tmp

import (
	"encoding/binary"
	"github.com/tobiasfamos/KVStore/kv"
	"github.com/tobiasfamos/KVStore/search"
	"github.com/tobiasfamos/KVStore/util"
)

/*
A page consists of <S> bytes and
- <1> uint32 (4 bytes) for page ID
- <1> byte for bitflag mask for page identification
- <3> bytes for 32-bit alignment
Therefore the data section for nodes consists of <S-8> bytes.

An InternalNode consists of
- <1> uint16 (2 bytes)
- <n> uint64 (8 bytes)
- <n+1> uint32 (4 bytes)
Therefore it takes (6 + 12n) bytes of storage space.
Solving with page size <S> for <n> we get:
	S - 8 = 2 + 8n + 4(n+1)
 =>     n = (S - 14) / 12
For a page size of 4096 we get <n> = 340

A LeafNode consists of
- <1> uint16 (2 bytes)
- <n> uint64 (8 bytes)
- <n> [10]byte (10 bytes)
Therefore it takes (2 + 18n) bytes of  we get:storage space.
Solving with page size <S> for <n> we get:
	S - 8 = 2 + 18n
 =>     n = (S - 10) / 18
For a page size of 4096 we get <n> = 227
*/

// KeyStartIndex is the starting index for keys for both InternalNode and LeafNode.
const KeyStartIndex = 2

// NumInternalKeys is the number of keys an InternalNode may hold at any given time.
const NumInternalKeys = (kv.PageSize - kv.PageMetadataSize - KeyStartIndex - 4) / 12

// NumInternalPages is the number of pages an InternalNode may hold at any given time.
const NumInternalPages = NumInternalKeys + 1

// InternalNodeSize is the size in bytes that an InternalNode uses.
const InternalNodeSize = KeyStartIndex + 4 + 12*NumInternalKeys

// NumLeafKeys is the number of keys a LeafNode may hold at any given time.
const NumLeafKeys = (kv.PageSize - kv.PageMetadataSize - KeyStartIndex) / 18

// NumLeafValues is the number of values a LeafNode may hold at any given time.
const NumLeafValues = NumLeafKeys

// LeafNodeSize is the size in bytes that a LeafNode uses.
const LeafNodeSize = KeyStartIndex + 18*NumLeafKeys

// PagesStartIndex is the starting index for the page IDs in InternalNode.
const PagesStartIndex = KeyStartIndex + NumInternalKeys*8

// ValuesStartIndex is the starting index for the values in LeafNode.
const ValuesStartIndex = KeyStartIndex + NumLeafKeys*8

// Encoder specifies an interface implemented by things which can be encoded
// into a page's worth o bytes.
type Encoder interface {
	encode() []byte
}

/*
InternalNode is a node that points NumInternalKeys keys to NumInternalKeys + 1 pages in a pyramid scheme.
The relationship uses less-or-equal for left-sided page IDs, greater for right-sided page IDs.

For <n = numKeys> used keys there must be <n+1> valid relations to the page IDs. Otherwise the node is corrupted.

An InternalNode takes at most InternalNodeSize bytes in memory.
*/
type InternalNode struct {
	keys    [NumInternalKeys]uint64
	pages   [NumInternalPages]kv.PageID
	numKeys uint16
}

/*
LeafNode is a node that points NumLeafKeys keys to NumLeafKeys values in a key-value relationship.

For <n = numKeys> used keys, there are also <n> values.

A LeafNode takes at most LeafNodeSize bytes in memory.
*/
type LeafNode struct {
	keys    [NumLeafKeys]uint64
	values  [NumLeafValues][10]byte
	numKeys uint16
}

// decodeInternalNode() decodes a byte slice of a least length InternalNodeSize into an InternalNode.
func decodeInternalNode(slice []byte) InternalNode {
	var _ = slice[InternalNodeSize-1] // bounds check hint to compiler; see golang.org/issue/14808

	var node InternalNode
	node.numKeys = binary.BigEndian.Uint16(slice[0:2])

	for i := 0; i < NumInternalKeys; i++ {
		var from = KeyStartIndex + i*8
		var to = from + 8
		node.keys[i] = binary.BigEndian.Uint64(slice[from:to])
	}

	for i := 0; i < NumInternalKeys+1; i++ {
		var from = PagesStartIndex + i*4
		var to = from + 4
		node.pages[i] = kv.PageID(binary.BigEndian.Uint32(slice[from:to]))
	}

	return node
}

// encode() encodes the InternalNode to a byte slice of length InternalNodeSize.
func (n *InternalNode) encode() []byte {
	var page = make([]byte, InternalNodeSize)
	binary.BigEndian.PutUint16(page[0:KeyStartIndex], n.numKeys)

	for i := 0; i < NumInternalKeys; i++ {
		var from = KeyStartIndex + i*8
		var to = from + 8
		binary.BigEndian.PutUint64(page[from:to], n.keys[i])
	}

	for i := 0; i < NumInternalKeys+1; i++ {
		var from = PagesStartIndex + i*4
		var to = from + 4
		binary.BigEndian.PutUint32(page[from:to], uint32(n.pages[i]))
	}

	return page
}

// decodeLeafNode() decodes a byte slice of a least length LeafNodeSize into a LeafNode.
func decodeLeafNode(page []byte) LeafNode {
	var _ = page[LeafNodeSize-1] // bounds check hint to compiler; see golang.org/issue/14808

	var node LeafNode
	node.numKeys = binary.BigEndian.Uint16(page[0:KeyStartIndex])

	for i := 0; i < NumLeafKeys; i++ {
		var from = KeyStartIndex + i*8
		var to = from + 8
		node.keys[i] = binary.BigEndian.Uint64(page[from:to])
	}

	for i := 0; i < NumLeafKeys; i++ {
		var from = ValuesStartIndex + i*10
		var to = from + 10
		copy(node.values[i][:], page[from:to])
	}

	return node
}

// encode() encodes the LeafNode to a byte slice of length LeafNodeSize.
func (n *LeafNode) encode() []byte {
	var page = make([]byte, LeafNodeSize)
	binary.BigEndian.PutUint16(page[0:KeyStartIndex], n.numKeys)

	for i := 0; i < NumLeafKeys; i++ {
		var from = KeyStartIndex + i*8
		var to = from + 8
		binary.BigEndian.PutUint64(page[from:to], n.keys[i])
	}

	for i := 0; i < NumLeafKeys; i++ {
		var from = ValuesStartIndex + i*10
		var to = from + 10
		copy(page[from:to], n.values[i][:])
	}

	return page
}

// isFull() returns whether this node is full.
func (n *InternalNode) isFull() bool {
	return n.numKeys == NumInternalKeys
}

// TODO: Add documentation
func (n *InternalNode) contains(s uint64) bool {
	_, found := search.Binary(s, n.keys[:n.numKeys])
	return found
}

// getPage() returns the page id associated with the given key
func (n *InternalNode) getPage(key uint64) kv.PageID {
	if n.numKeys == 0 {
		panic("InternalNode should not be empty")
	}

	// todo: performance
	for i := uint16(0); i < n.numKeys; i++ {
		if n.keys[i] >= key {
			return n.pages[i]
		}
	}

	return n.pages[n.numKeys]
}

// TODO: Add documentation. Raw unsafe method. caller must ensure tree coherence in regards to separator
func (n *InternalNode) leftInsertPage(s uint64, id kv.PageID) bool {
	if n.isFull() {
		return false
	}

	idx, found := search.Binary(s, n.keys[:n.numKeys])
	if found {
		return false
	}

	// move everything from idx onwards one up
	copy(n.keys[idx+1:n.numKeys+1], n.keys[idx:n.numKeys])
	copy(n.pages[idx+1:n.numKeys+1], n.pages[idx:n.numKeys])

	n.keys[idx] = s
	n.pages[idx] = id
	n.numKeys++

	return true
}

/*
split splits the node into two of equal size in a best effort.

The left node is in INCONSISTENT STATE, as it has <n> keys mapped to <n> pages such that the FURTHERMOST RIGHT PageID is missing.

It returns (left, right, separator)
*/
func (n *InternalNode) split() (InternalNode, InternalNode, uint64) {
	middle := n.numKeys / 2

	left := InternalNode{}
	left.numKeys = middle
	copy(left.keys[:], n.keys[:middle])
	copy(left.pages[:], n.pages[:middle])

	right := InternalNode{}
	right.numKeys = n.numKeys - middle
	copy(right.keys[:], n.keys[middle:])
	copy(right.pages[:], n.pages[middle:])

	lastValid := util.Max(0, left.numKeys-1)
	separator := (left.keys[lastValid] + right.keys[0]) / 2

	return left, right, separator
}

// isFull() returns whether the node is full.
func (n *LeafNode) isFull() bool {
	return n.numKeys == NumLeafKeys
}

// contains() performs linear search for the given key.
func (n *LeafNode) contains(key uint64) bool {
	for i := uint16(0); i < n.numKeys; i++ {
		if n.keys[i] == key {
			return true
		}
	}
	return false
}

/*
get() performs a linear search for the given key.
If the association is found it returns (value, true).
If it wasn't found, it returns (empty bytes, false).
*/
func (n *LeafNode) get(key uint64) ([10]byte, bool) {
	for i := uint16(0); i < n.numKeys; i++ {
		if n.keys[i] == key {
			return n.values[i], true
		}
	}
	return [10]byte{2}, false
}

/*
insert() inserts a key with a given value into this node.
If the node is already isFull() or the node already contains() the key,
nothing will be done and the method returns false.
Otherwise, the key-value pair will be inserted, preserving the order inside this node,
and the method returns true.
*/
func (n *LeafNode) insert(key uint64, value [10]byte) bool {
	if n.isFull() || n.contains(key) {
		return false
	}

	// search for suitable place in existing keys
	for i := uint16(0); i < n.numKeys; i++ {
		if n.keys[i] > key {
			// move all other key/values one up
			for j := n.numKeys; j > i; j-- {
				n.keys[j] = n.keys[j-1]
				n.values[j] = n.values[j-1]
			}

			n.keys[i] = key
			n.values[i] = value
			n.numKeys++

			return true
		}
	}

	// No suitable place inbetween
	n.keys[n.numKeys] = key
	n.values[n.numKeys] = value
	n.numKeys++

	return true
}

/*
split splits the node into two of equal size in a best effort.

It returns (left, right, separator)
*/
func (n *LeafNode) split() (LeafNode, LeafNode, uint64) {
	middle := n.numKeys / 2

	left := LeafNode{}
	left.numKeys = middle
	copy(left.keys[:], n.keys[:middle])
	copy(left.values[:], n.values[:middle])

	right := LeafNode{}
	right.numKeys = n.numKeys - middle
	copy(right.keys[:], n.keys[middle:])
	copy(right.values[:], n.values[middle:])

	lastValid := util.Max(0, left.numKeys-1)
	separator := (left.keys[lastValid] + right.keys[0]) / 2

	return left, right, separator
}
