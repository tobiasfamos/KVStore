package tmp

import (
	"bytes"
	"github.com/tobiasfamos/KVStore/kv"
	"testing"
)

const (
	testNumKeys = 2

	testFirstKey  = 1
	testSecondKey = 2
	testThirdKey  = 3

	testFirstPageID  = kv.PageID(1)
	testSecondPageID = kv.PageID(2)
	testThirdPageID  = kv.PageID(3)
)

var (
	testKeys    = []uint64{testFirstKey, testSecondKey, testThirdKey}
	testPageIDs = []kv.PageID{testFirstPageID, testSecondPageID, testThirdPageID}

	testFirstValue  = [10]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 10}
	testSecondValue = [10]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 20}
	testThirdValue  = [10]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 30}
	testValues      = [][10]byte{testFirstValue, testSecondValue, testThirdValue}
)

type testNodes struct {
	page         kv.Page
	InternalNode *InternalNode
	leafNode     *LeafNode
	isEmpty      bool
	isFull       bool
}

func testInternalNode() ([kv.PageDataSize]byte, InternalNode) {
	internalData := [kv.PageDataSize]byte{
		0, testNumKeys, // numKeys (2 bytes)
		0, 0, 0, 0, 0, 0, 0, testFirstKey, // first key (8 bytes)
		0, 0, 0, 0, 0, 0, 0, testSecondKey, // second key (8 bytes)
	}

	copy(internalData[PagesStartIndex:], []byte{
		0, 0, 0, byte(testFirstPageID), // first page ID
		0, 0, 0, byte(testSecondPageID),
		0, 0, 0, byte(testThirdPageID),
	})
	internalNode := InternalNode{
		keys:    [NumInternalKeys]uint64{testFirstKey, testSecondKey},
		pages:   [NumInternalPages]kv.PageID{testFirstPageID, testSecondPageID, testThirdPageID},
		numKeys: testNumKeys,
	}
	return internalData, internalNode
}

func testLeafNode() ([kv.PageDataSize]byte, LeafNode) {
	leafData := [kv.PageDataSize]byte{
		0, testNumKeys, // numKeys (2 bytes)
		0, 0, 0, 0, 0, 0, 0, testFirstKey, // first key (8 bytes)
		0, 0, 0, 0, 0, 0, 0, testSecondKey, // second key (8 bytes)
	}
	copy(leafData[ValuesStartIndex:], append(testFirstValue[:], testSecondValue[:]...))
	leafNode := LeafNode{
		keys:    [NumLeafKeys]uint64{testFirstKey, testSecondKey},
		values:  [NumLeafValues][10]byte{testFirstValue, testSecondValue},
		numKeys: testNumKeys,
	}
	return leafData, leafNode
}

func exampleNodes() []testNodes {
	iData, iNode := testInternalNode()
	lData, lNode := testLeafNode()
	return []testNodes{
		{kv.Page{}, &InternalNode{}, nil, true, false},
		{kv.Page{isLeaf: true}, nil, &LeafNode{}, true, false},
		{kv.Page{data: iData}, &iNode, nil, false, false},
		{kv.Page{isLeaf: true, data: lData}, nil, &lNode, false, false},
	}
}

func TestNode_IsFull(t *testing.T) {
	nodes := exampleNodes()

	for _, node := range nodes {
		if node.page.isLeaf {
			lNode := decodeLeafNode(node.page.data[:])
			if lNode.isFull() != node.isFull {
				t.Errorf("Actual isFull = %t, Expected == %t", lNode.isFull(), node.isFull)
			}
		} else {
			iNode := decodeInternalNode(node.page.data[:])
			if iNode.isFull() != node.isFull {
				t.Errorf("Actual isFull = %t, Expected == %t", iNode.isFull(), node.isFull)
			}
		}
	}
}

func TestNode_Decode(t *testing.T) {
	nodes := exampleNodes()

	for _, node := range nodes {
		if node.page.isLeaf {
			lNode := decodeLeafNode(node.page.data[:])
			if lNode != *node.leafNode {
				t.Errorf("Decoding LeafNode fails")
			}
		} else {
			iNode := decodeInternalNode(node.page.data[:])
			if iNode != *node.InternalNode {
				t.Errorf("Decoding InternalNode fails")
			}
		}
	}
}

func TestNode_Encode(t *testing.T) {
	nodes := exampleNodes()

	for _, node := range nodes {
		if node.page.isLeaf {
			data := node.leafNode.encode()
			if !bytes.Equal(data, node.page.data[:LeafNodeSize]) {
				t.Errorf("Encoding LeafNode fails")
			}
		} else {
			data := node.InternalNode.encode()
			if !bytes.Equal(data, node.page.data[:InternalNodeSize]) {
				t.Errorf("Encoding LeafNode fails")
			}
		}
	}
}

func TestInternalNode_GetPage(t *testing.T) {
	nodes := exampleNodes()

	for _, node := range nodes {
		if node.isEmpty || node.page.isLeaf {
			continue
		}

		for i := 0; i < testNumKeys; i++ {
			pageID := node.InternalNode.getPage(testKeys[i])
			if pageID != testPageIDs[i] {
				t.Errorf("Actual pageID = %x, Expected = %x", pageID, testPageIDs[i])
			}

			iNode := decodeInternalNode(node.page.data[:])
			pageID = iNode.getPage(testKeys[i])
			if pageID != testPageIDs[i] {
				t.Errorf("Actual pageID = %x, Expected = %x", pageID, testPageIDs[i])
			}
		}
	}
}

func TestLeafNode_Get(t *testing.T) {
	nodes := exampleNodes()

	for _, node := range nodes {
		if node.isEmpty || !node.page.isLeaf {
			continue
		}

		for i := 0; i < testNumKeys; i++ {
			value, found := node.leafNode.get(testKeys[i])
			if !found {
				t.Errorf("Actual found key = false, Expected == true")
			}
			if value != testValues[i] {
				t.Errorf("Actual value = %x, Expected = %x", value, testValues[i])
			}

			lNode := decodeLeafNode(node.page.data[:])
			value, found = lNode.get(testKeys[i])
			if !found {
				t.Errorf("Actual found key = false, Expected == true")
			}
			if value != testValues[i] {
				t.Errorf("Actual value = %x, Expected = %x", value, testValues[i])
			}
		}
	}
}

func TestLeafNode_Insert(t *testing.T) {
	nodes := exampleNodes()

	for _, node := range nodes {
		if !node.isEmpty || !node.page.isLeaf {
			continue
		}
		decoded := decodeLeafNode(node.page.data[:])

		for _, lNode := range []*LeafNode{node.leafNode, &decoded} {
			lNode.insert(testSecondKey, testSecondValue)
			// insert key 2 => [2]
			{
				lNode.insert(testSecondKey, testSecondValue)
				if lNode.numKeys != 1 {
					t.Errorf("Actual numKeys = %d, Expected == 1", lNode.numKeys)
				}
				if lNode.keys[0] != testSecondKey {
					t.Errorf("Actual keys = [%d], Expected == [%d]", lNode.keys[0], testSecondKey)
				}
				if lNode.values[0] != testSecondValue {
					t.Errorf("Actual values = %v, Expected == %v", lNode.values, testSecondValue)
				}

				var secondValue, secondFound = lNode.get(testSecondKey)
				if !secondFound {
					t.Errorf("Actual secondFound = %t, Expected == true", secondFound)
				}
				if secondValue != testSecondValue {
					t.Errorf("Actual second value = %v, Expected == %v", secondValue, testSecondValue)
				}
			}

			// insert key 1 => [1, 2]
			{
				lNode.insert(testFirstKey, testFirstValue)
				if lNode.numKeys != 2 {
					t.Errorf("Actual numKeys = %d, Expected == 2", lNode.numKeys)
				}
				if lNode.keys[0] != testFirstKey || lNode.keys[1] != testSecondKey {
					t.Errorf("Actual keys = %v, Expected == %v", lNode.keys[0:2], []uint64{testFirstKey, testSecondKey})
				}
				if lNode.values[0] != testFirstValue || lNode.values[1] != testSecondValue {
					t.Errorf("Actual values = %v, Expected == %v", lNode.values[0:2], [][10]byte{testFirstValue, testSecondValue})
				}

				var firstValue, firstFound = lNode.get(testFirstKey)
				if !firstFound {
					t.Errorf("Actual secondFound = %t, Expected == true", firstFound)
				}
				if firstValue != testFirstValue {
					t.Errorf("Actual first value = %v, Expected == %v", firstValue, testFirstValue)
				}
			}

			// insert key 3 => [1, 2, 3]
			{
				lNode.insert(testThirdKey, testThirdValue)
				if lNode.numKeys != 3 {
					t.Errorf("Actual numKeys = %d, Expected == 3", lNode.numKeys)
				}
				if lNode.keys[0] != testFirstKey || lNode.keys[1] != testSecondKey || lNode.keys[2] != testThirdKey {
					t.Errorf("Actual keys = %v, Expected == %v", lNode.keys[0:3], []uint64{testFirstKey, testSecondKey, testThirdKey})
				}
				if lNode.values[0] != testFirstValue || lNode.values[1] != testSecondValue || lNode.values[2] != testThirdValue {
					t.Errorf("Actual values = %v, Expected == %v", lNode.values[0:3], [][10]byte{testFirstValue, testSecondValue, testThirdValue})
				}

				var thirdValue, thirdFound = lNode.get(testThirdKey)
				if !thirdFound {
					t.Errorf("Actual thirdFound = %t, Expected == true", thirdFound)
				}
				if thirdValue != testThirdValue {
					t.Errorf("Actual first value = %v, Expected == %v", thirdValue, testThirdValue)
				}
			}
		}
	}
}
