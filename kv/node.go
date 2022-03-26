package kv

import (
	"encoding/binary"
)

/*
following results stem from a 4096-byte page with
- 4-byte page ID
- 1 byte bitflag mask
*/

// PageSize is the default page size for both InternalNode and LeafNode to reside in.
const PageSize = 4096

// NodeDataStartIndex is the starting index for the actual node information for both InternalNode and LeafNode.
const NodeDataStartIndex = 5

// NumInternalKeys is the number of keys an InternalNode may hold at any given time.
const NumInternalKeys = 340

// InternalNodeSize is the size in bytes that an InternalNode uses.
const InternalNodeSize = 4086

// NumLeafKeys is the number of keys a LeafNode may hold at any given time.
const NumLeafKeys = 227

// LeafNodeSize is the size in bytes that a LeafNode uses.
const LeafNodeSize = 4088

// KeyStartIndex is the starting index for keys for both InternalNode and LeafNode.
const KeyStartIndex = 2

// PagesStartIndex is the starting index for the page IDs in InternalNode.
const PagesStartIndex = KeyStartIndex + NumInternalKeys*8

// ValuesStartIndex is the starting index for the values in LeafNode.
const ValuesStartIndex = KeyStartIndex + NumLeafKeys*8

/*
InternalNode is a node that points NumInternalKeys keys to NumInternalKeys + 1 pages in a pyramid scheme.
The relationship uses less-or-equal for left-sided page IDs, greater for right-sided page IDs.

For <n = numKeys> used keys there must be <n+1> valid relations to the page IDs. Otherwise the node is corrupted.

An InternalNode takes at most InternalNodeSize bytes in memory.
*/
type InternalNode struct {
	numKeys uint16
	keys    [NumInternalKeys]uint64
	pages   [NumInternalKeys + 1]uint32
}

/*
LeafNode is a node that points NumLeafKeys keys to NumLeafKeys values in a key-value relationship.

For <n = numKeys> used keys, there are also <n> values.

A LeafNode takes at most LeafNodeSize bytes in memory.
*/
type LeafNode struct {
	numKeys uint16
	keys    [NumLeafKeys]uint64
	values  [NumLeafKeys][10]byte
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
		node.pages[i] = binary.BigEndian.Uint32(slice[from:to])
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
		binary.BigEndian.PutUint32(page[from:to], n.pages[i])
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

// getPage() returns the page id associated with the given key
func (n *InternalNode) getPage(key uint64) uint32 {
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
	return [10]byte{}, false
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
