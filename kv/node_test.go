package kv

import (
	"encoding/binary"
	"math"
	"testing"
)

const testNumKeys = 2
const testFirstKey = 1
const testSecondKey = 2
const testThirdKey = 3
const testFirstPageID = 1
const testSecondPageID = 2
const testThirdPageID = 3

var testFirstValue = [10]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 10}
var testSecondValue = [10]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 20}
var testThirdValue = [10]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 30}

func exampleInternalNodePage() [PageSize]byte {
	var page = [PageSize]byte{
		0b0, 0b0, 0b0, 0b0, // page ID
		0b0,              // bitflag
		0b0, testNumKeys, // numKeys (2 bytes)
		0b0, 0b0, 0b0, 0b0, 0b0, 0b0, 0b0, testFirstKey, // first key (8 bytes)
		0b0, 0b0, 0b0, 0b0, 0b0, 0b0, 0b0, testSecondKey, // second key (8 bytes)
	}
	var i = NodeDataStartIndex + PagesStartIndex + 3 // first page ID (last byte of uint32)
	page[i] = testFirstPageID
	page[i+4] = testSecondPageID
	page[i+8] = testThirdPageID

	return page
}

func fakeFullInternalNodePage() [PageSize]byte {
	var page = [PageSize]byte{
		0b0, 0b0, 0b0, 0b0, // page ID
		0b0,                          // bitflag
		math.MaxUint8, math.MaxUint8, // numKeys FULL (2 bytes)
	}
	binary.BigEndian.PutUint16(page[NodeDataStartIndex:NodeDataStartIndex+2], NumInternalKeys)

	return page
}

func exampleLeafNodePage() [PageSize]byte {
	var page = [PageSize]byte{
		0b0, 0b0, 0b0, 0b0, // page ID
		0b1,              // bitflag
		0b0, testNumKeys, // numKeys (2 bytes)
		0b0, 0b0, 0b0, 0b0, 0b0, 0b0, 0b0, testFirstKey, // first key (8 bytes)
		0b0, 0b0, 0b0, 0b0, 0b0, 0b0, 0b0, testSecondKey, // second key (8 bytes)
	}
	var i = NodeDataStartIndex + ValuesStartIndex // first value ([10]byte)
	copy(page[i:i+10], testFirstValue[:])
	i += 10
	copy(page[i:i+10], testSecondValue[:])

	return page
}

func fakeFullLeafNodePage() [PageSize]byte {
	var page = [PageSize]byte{
		0b0, 0b0, 0b0, 0b0, // page ID
		0b1, // bitflag
	}
	binary.BigEndian.PutUint16(page[NodeDataStartIndex:NodeDataStartIndex+2], NumLeafKeys)

	return page
}

// test empty InternalNode is not full
func TestEmptyInternalNodeIsNotFull(t *testing.T) {
	var nodeData = make([]byte, InternalNodeSize)
	var node = decodeInternalNode(nodeData)

	if node.isFull() {
		t.Errorf("Actual isFull = %t, Expected == false", node.isFull())
	}
}

// test InternalNode example is not full
func TestInternalNodeExampleIsNotFull(t *testing.T) {
	var page = exampleInternalNodePage()
	var node = decodeInternalNode(page[NodeDataStartIndex:])

	if node.isFull() {
		t.Errorf("Actual isFull = %t, Expected == false", node.isFull())
	}
}

// test fake full InternalNode is full
func TestFakeFullInternalNodeIsFull(t *testing.T) {
	var page = fakeFullInternalNodePage()
	var node = decodeInternalNode(page[NodeDataStartIndex:])

	if !node.isFull() {
		t.Errorf("Actual isFull = %t, Expected == true", node.isFull())
	}
}

// test decode empty InternalNode
func TestDecodeEmptyInternalNode(t *testing.T) {
	var nodeData = make([]byte, InternalNodeSize)
	var node = decodeInternalNode(nodeData)

	if node.numKeys != 0 {
		t.Errorf("Actual numKeys = %d, Expected == 0", node.numKeys)
	}
}

// test decode InternalNode example
func TestDecodeInternalNodeExample(t *testing.T) {
	var page = exampleInternalNodePage()
	var node = decodeInternalNode(page[NodeDataStartIndex:])

	if node.numKeys != testNumKeys {
		t.Errorf("Actual numKeys = %d, Expected == %d", node.numKeys, testNumKeys)
	}
	if node.keys[0] != testFirstKey {
		t.Errorf("Actual first key = %d, Expected == %d", node.keys[0], testFirstKey)
	}
	if node.keys[1] != testSecondKey {
		t.Errorf("Actual second key = %d, Expected == %d", node.keys[1], testSecondKey)
	}

	var firstID = node.getPage(testFirstKey)
	if firstID != testFirstPageID {
		t.Errorf("Actual first page ID = %d, Expected == %d", firstID, testFirstPageID)
	}
	var secondID = node.getPage(testSecondKey)
	if secondID != testSecondPageID {
		t.Errorf("Actual second page ID = %d, Expected == %d", secondID, testSecondPageID)
	}
	var thirdID = node.getPage(testThirdKey)
	if thirdID != testThirdPageID {
		t.Errorf("Actual third page ID = %d, Expected == %d", thirdID, testThirdPageID)
	}
}

// test empty InternalNode conversion (decode, encode, decode)
func TestEmptyInternalNodeConversion(t *testing.T) {
	var nodeData = make([]byte, InternalNodeSize)

	var node = decodeInternalNode(nodeData)
	var encoded = node.encode()
	var node2 = decodeInternalNode(encoded)

	if node != node2 {
		t.Errorf("Both nodes should be the same")
	}
}

// test InternalNode example conversion (decode, encode, decode)
func TestInternalNodeExampleConversion(t *testing.T) {
	var page = exampleInternalNodePage()

	var node = decodeInternalNode(page[NodeDataStartIndex:])
	var encoded = node.encode()
	var node2 = decodeInternalNode(encoded)

	if node != node2 {
		t.Errorf("Both nodes should be the same")
	}
}

// test LeafNode example is not full
func TestLeafNodeExampleIsNotFull(t *testing.T) {
	var page = exampleLeafNodePage()
	var node = decodeLeafNode(page[NodeDataStartIndex:])

	if node.isFull() {
		t.Errorf("Actual isFull = %t, Expected == false", node.isFull())
	}
}

// test fake full LeafNode is full
func TestFakeFullLeafNodeIsFull(t *testing.T) {
	var page = fakeFullLeafNodePage()
	var node = decodeLeafNode(page[NodeDataStartIndex:])

	if !node.isFull() {
		t.Errorf("Actual isFull = %t, Expected == true", node.isFull())
	}
}

// test decode empty LeafNode
func TestDecodeEmptyLeafNode(t *testing.T) {
	var nodeData = make([]byte, LeafNodeSize)
	var node = decodeLeafNode(nodeData)

	if node.numKeys != 0 {
		t.Errorf("Actual numKeys = %d, Expected == 0", node.numKeys)
	}
}

// test decode LeafNode example
func TestDecodeLeafNodeExample(t *testing.T) {
	var page = exampleLeafNodePage()
	var node = decodeLeafNode(page[NodeDataStartIndex:])

	if node.numKeys != testNumKeys {
		t.Errorf("Actual numKeys = %d, Expected == %d", node.numKeys, testNumKeys)
	}
	if node.keys[0] != testFirstKey {
		t.Errorf("Actual first key = %d, Expected == %d", node.keys[0], testFirstKey)
	}
	if node.keys[1] != testSecondKey {
		t.Errorf("Actual first key = %d, Expected == %d", node.keys[0], testFirstKey)
	}

	if !node.contains(testFirstKey) {
		t.Errorf("Actual contains first key = false, Expected == true")
	}
	if !node.contains(testSecondKey) {
		t.Errorf("Actual contains second key = false, Expected == true")
	}
	if node.contains(testThirdKey) {
		t.Errorf("Actual contains second key = true, Expected == false")
	}

	var firstValue, firstFound = node.get(testFirstKey)
	if !firstFound {
		t.Errorf("Actual firstFound = %t, Expected == true", firstFound)
	}
	if firstValue != testFirstValue {
		t.Errorf("Actual first value = %v, Expected == %v", firstValue, testFirstValue)
	}

	var secondValue, secondFound = node.get(testSecondKey)
	if !secondFound {
		t.Errorf("Actual firstFound = %t, Expected == true", secondFound)
	}
	if secondValue != testSecondValue {
		t.Errorf("Actual first value = %v, Expected == %v", secondValue, testSecondValue)
	}
}

// test empty LeafNode conversion (decode, encode, decode)
func TestEmptyLeafNodeConversion(t *testing.T) {
	var nodeData = make([]byte, LeafNodeSize)

	var node = decodeLeafNode(nodeData)
	var encoded = node.encode()
	var node2 = decodeLeafNode(encoded)

	if node != node2 {
		t.Errorf("Both nodes should be the same")
	}
}

// test LeafNode example conversion (decode, encode, decode)
func TestLeafNodeExampleConversion(t *testing.T) {
	var page = exampleLeafNodePage()

	var node = decodeLeafNode(page[NodeDataStartIndex:])
	var encoded = node.encode()
	var node2 = decodeLeafNode(encoded)

	if node != node2 {
		t.Errorf("Both nodes should be the same")
	}
}

// test inserting 3 values to an empty leaf node
func TestInsertToEmptyLeafNode(t *testing.T) {
	var node LeafNode

	// insert key 2 => [2]
	{
		node.insert(testSecondKey, testSecondValue)
		if node.numKeys != 1 {
			t.Errorf("Actual numKeys = %d, Expected == 1", node.numKeys)
		}
		if node.keys[0] != testSecondKey {
			t.Errorf("Actual keys = [%d], Expected == [%d]", node.keys[0], testSecondKey)
		}
		if node.values[0] != testSecondValue {
			t.Errorf("Actual values = %v, Expected == %v", node.values, testSecondValue)
		}

		var secondValue, secondFound = node.get(testSecondKey)
		if !secondFound {
			t.Errorf("Actual secondFound = %t, Expected == true", secondFound)
		}
		if secondValue != testSecondValue {
			t.Errorf("Actual second value = %v, Expected == %v", secondValue, testSecondValue)
		}
	}

	// insert key 1 => [1, 2]
	{
		node.insert(testFirstKey, testFirstValue)
		if node.numKeys != 2 {
			t.Errorf("Actual numKeys = %d, Expected == 2", node.numKeys)
		}
		if node.keys[0] != testFirstKey || node.keys[1] != testSecondKey {
			t.Errorf("Actual keys = %v, Expected == %v", node.keys[0:2], []uint64{testFirstKey, testSecondKey})
		}
		if node.values[0] != testFirstValue || node.values[1] != testSecondValue {
			t.Errorf("Actual values = %v, Expected == %v", node.values[0:2], [][10]byte{testFirstValue, testSecondValue})
		}

		var firstValue, firstFound = node.get(testFirstKey)
		if !firstFound {
			t.Errorf("Actual secondFound = %t, Expected == true", firstFound)
		}
		if firstValue != testFirstValue {
			t.Errorf("Actual first value = %v, Expected == %v", firstValue, testFirstValue)
		}
	}

	// insert key 3 => [1, 2, 3]
	{
		node.insert(testThirdKey, testThirdValue)
		if node.numKeys != 3 {
			t.Errorf("Actual numKeys = %d, Expected == 3", node.numKeys)
		}
		if node.keys[0] != testFirstKey || node.keys[1] != testSecondKey || node.keys[2] != testThirdKey {
			t.Errorf("Actual keys = %v, Expected == %v", node.keys[0:3], []uint64{testFirstKey, testSecondKey, testThirdKey})
		}
		if node.values[0] != testFirstValue || node.values[1] != testSecondValue || node.values[2] != testThirdValue {
			t.Errorf("Actual values = %v, Expected == %v", node.values[0:3], [][10]byte{testFirstValue, testSecondValue, testThirdValue})
		}

		var thirdValue, thirdFound = node.get(testThirdKey)
		if !thirdFound {
			t.Errorf("Actual thirdFound = %t, Expected == true", thirdFound)
		}
		if thirdValue != testThirdValue {
			t.Errorf("Actual first value = %v, Expected == %v", thirdValue, testThirdValue)
		}
	}
}
