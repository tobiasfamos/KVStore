package kv

import (
	"encoding/binary"
	"errors"
	"io/ioutil"
	"math/rand"
	"os"
	"testing"
)

var helper = TestHelper{}

func TestMain(m *testing.M) {
	// Initialze helper before running test, and call its cleanup before
	// terminating.
	helper.Initialize()
	result := m.Run()
	helper.Cleanup()

	os.Exit(result)
}

func TestIntfSize(t *testing.T) {
	tests := []struct {
		size       int
		expectFail bool
	}{
		{0, false},
		{100, false},
		{MaxMem + 1, true},
	}

	for _, test := range tests {
		_, err := NewKvStoreInstance(test.size, ".")
		if (err == nil) && test.expectFail {
			t.Errorf(
				"Size = %d, Expected fail == %t, got none",
				test.size,
				test.expectFail,
			)
		}
	}
}

func TestGetAndPut(t *testing.T) {
	// Happy-case Get() and Put() will be tested together, as there's
	// really no way to do one without the other.
	kv, _ := helper.GetEmptyInstance()

	tests := []struct {
		key   uint64
		value [10]byte
	}{
		{1, [10]byte{1, 2, 3}},
		{2, [10]byte{0, 0, 1, 1, 0, 0, 1, 1, 0, 0}},
		{3, [10]byte{104, 101, 108, 108, 111, 119, 111, 114, 108, 100}},
	}

	// Put all values
	for _, test := range tests {
		err := kv.Put(test.key, test.value)
		if err != nil {
			t.Errorf("Error putting element (%d = %v): %v", test.key, test.value, err)
		}
	}

	// Now read them and ensure they are as expected
	for _, test := range tests {
		val, err := kv.Get(test.key)
		if err != nil {
			t.Errorf("Error getting element %d: %v", test.key, err)
		}

		if val != test.value {
			t.Errorf(
				"Got unexpected value %v for key %d; expected %v",
				val,
				test.key,
				test.value,
			)
		}
	}
}

func TestBufferPoolNewPage(t *testing.T) {
	memorySize := 4096 * 10
	numberOfPages := memorySize / PageSize
	newCacheEviction := NewLRUCache(12)
	newRamDisk := NewRAMDisk(uint(memorySize), 12)
	localBufferPool := NewBufferPool(uint(numberOfPages), newRamDisk, &newCacheEviction)

	page1, _ := localBufferPool.NewPage()
	localBufferPool.UnpinPage(page1.id, true)
	page2, _ := localBufferPool.NewPage()
	localBufferPool.UnpinPage(page2.id, true)
	page3, _ := localBufferPool.NewPage()
	localBufferPool.UnpinPage(page3.id, true)

	page1Fetch, _ := localBufferPool.FetchPage(page1.id)
	page2Fetch, _ := localBufferPool.FetchPage(page2.id)
	page3Fetch, _ := localBufferPool.FetchPage(page3.id)
	if page1Fetch.id == page2Fetch.id {
		t.Errorf("Same Ids, expected different")
	}
	if page3Fetch.id == page2Fetch.id {
		t.Errorf("Same IDs, expected differen")
	}

}

func TestForceLeafNodeSplitOnce(t *testing.T) {
	// Test inserting many elements to force a node split
	kv, _ := helper.GetEmptyInstance()

	// Insert keys to the left node
	for i := uint64(0); i < NumLeafKeys; i += 1 {
		a := [10]byte{}
		binary.LittleEndian.PutUint64(a[:], i)
		err := kv.Put(i, a)
		if err != nil {
			t.Errorf("Expected no error when putting elements; Got %v", err)
		}
	}

	a := [10]byte{}
	binary.LittleEndian.PutUint32(a[:], uint32(NumLeafKeys))
	err := kv.Put(uint64(NumLeafKeys), a)
	if err != nil {
		t.Errorf("Expected no error when putting elements; Got %v", err)
	}

	tests := []struct {
		key uint64
	}{
		{0},
		{NumLeafKeys / 8},
		{NumLeafKeys / 6},
		{NumLeafKeys / 4},
		{NumLeafKeys / 2},
		{3 * NumLeafKeys / 4},
		{NumLeafKeys},
	}

	// Now read them and ensure they are as expected
	for _, test := range tests {
		val, err := kv.Get(test.key)
		if err != nil {
			t.Errorf("Error getting element %d: %v", test.key, err)
		}
		convertedVal := binary.LittleEndian.Uint64(val[:])
		if convertedVal != test.key {
			t.Errorf(
				"Got unexpected value %d for key %d; expected %d",
				val,
				test.key,
				test.key,
			)
		}
	}
}

func TestForceLeafNodeSplitFourTimes(t *testing.T) {
	t.Skip("Not working yet, is in an endless loop. Perhaps a new node is not seen as a LNode and it gets stuck.")

	// Test inserting many elements to force a node split
	kv, _ := helper.GetEmptyInstance()
	for i := uint64(0); i < NumLeafKeys*4+1; i += 1 {
		a := [10]byte{}
		binary.LittleEndian.PutUint64(a[:], i)
		err := kv.Put(i, a)
		if err != nil {
			t.Errorf("Expected no error when putting key: %d; Got %v", i, err)
		}
	}

	tests := []struct {
		key uint64
	}{
		{0},
		{4 * NumLeafKeys / 8},
		{4 * NumLeafKeys / 6},
		{4 * NumLeafKeys / 4},
		{4 * NumLeafKeys / 2},
		{4 * 3 * NumLeafKeys / 4},
		{4 * NumLeafKeys},
	}

	// Now read them and ensure they are as expected
	for _, test := range tests {
		val, err := kv.Get(test.key)
		if err != nil {
			t.Errorf("Error getting element %d: %v", test.key, err)
		}
		convertedVal := binary.LittleEndian.Uint64(val[:])
		if convertedVal != test.key {
			t.Errorf(
				"Got unexpected value %d for key %d; expected %d",
				val,
				test.key,
				test.key,
			)
		}
	}
}

func TestPutKeyRandomly(t *testing.T) {
	const numberOfKeysToInsert = NumLeafKeys
	// Put Keys in randomly to test the splitting of nodes.
	r := rand.New(rand.NewSource(99))

	r1 := rand.New(rand.NewSource(99))

	kv, _ := helper.GetEmptyInstance()
	for i := 0; i < numberOfKeysToInsert; i += 1 {
		a := [10]byte{}
		keyToPut := r.Uint32()
		binary.LittleEndian.PutUint32(a[:], keyToPut)
		err := kv.Put(uint64(keyToPut), a)
		if err != nil {
			t.Errorf("Expected no error when putting key: %d; Got %v", i, err)
		}
	}

	// Now read them and ensure they are as expected
	for i := 0; i < numberOfKeysToInsert; i += 1 {
		expected := r1.Uint32()
		val, err := kv.Get(uint64(expected))
		if err != nil {
			t.Fatalf("Index: %dError getting element %d: %v", i, expected, err)
		}
		convertedVal := binary.LittleEndian.Uint32(val[:])
		if convertedVal != expected {
			t.Errorf(
				"Index: %d, Got unexpected value %d for key %d; expected %d",
				i,
				convertedVal,
				expected,
				expected,
			)
		}
	}

}

func TestPutKeyRandomlyMany(t *testing.T) {
	const numberOfKeysToInsert = NumLeafKeys*4 + 1
	// Put Keys in randomly to test the splitting of nodes.
	InsertRandom(t, numberOfKeysToInsert)

}

func TestSplitRootNode(t *testing.T) {
	InsertRandom(t, 100000)
}

func InsertRandom(t *testing.T, numberOfKeysToInsert int) {
	r := rand.New(rand.NewSource(99))

	r1 := rand.New(rand.NewSource(99))

	kv, _ := helper.GetEmptyInstance()
	for i := 0; i < numberOfKeysToInsert; i += 1 {
		a := [10]byte{}
		keyToPut := r.Uint32()
		binary.LittleEndian.PutUint32(a[:], uint32(keyToPut))
		err := kv.Put(uint64(keyToPut), a)
		if err != nil {
			t.Errorf("Expected no error when putting key: %d; Got %v", i, err)
		}
	}

	// Now read them and ensure they are as expected
	for i := 0; i < numberOfKeysToInsert; i += 1 {
		expected := r1.Uint32()
		val, err := kv.Get(uint64(expected))
		if err != nil {
			t.Errorf("Index %d: Error getting element %d: %v", i, expected, err)
		}
		convertedVal := binary.LittleEndian.Uint32(val[:])
		if convertedVal != expected {
			t.Errorf(
				"Index: %d, Got unexpected value %d for key %d; expected %d",
				i,
				convertedVal,
				expected,
				expected,
			)
		}
	}
}

func TestPutExistingElement(t *testing.T) {
	kv, _ := helper.GetEmptyInstance()

	err := kv.Put(1, [10]byte{})
	if err != nil {
		t.Errorf("Error putting element: %v", err)
	}

	err = kv.Put(1, [10]byte{})
	if err == nil {
		t.Errorf("Expected error when putting existing element; got none")
	}
}

func TestGetNonexistantElement(t *testing.T) {
	kv, _ := helper.GetEmptyInstance()

	_, err := kv.Get(1)
	if err == nil {
		t.Errorf("Expected error when getting nonexistant element; got none")
	}
}

func TestGetPutExceedingMemory(t *testing.T) {
	t.Skip("Skipping expected-failing test")
	kv, _ := helper.GetEmptyInstanceWithMemoryLimit(1000)

	// Each key/value pair will use up 8+10 = 18 bytes, so <56 will fit in
	// memory.
	// As such we'll put 100 key-value pairs, which is guaranteed to
	// overflow to disk.
	for i := uint64(0); i < 100; i++ {
		err := kv.Put(i, [10]byte{byte(i)})
		if err != nil {
			t.Errorf("Error putting element %d: %v", i, err)
		}
	}

	// Make sure they're all present
	for i := uint64(0); i < 100; i++ {
		val, err := kv.Get(i)
		if err != nil {
			t.Errorf("Error getting element %d: %v", i, err)
		}

		if val != [10]byte{byte(i)} {
			// We'll abort early so as not to spam the log with test failures
			t.Fatalf(
				"Got unexpected value %v for key %d; expected %v",
				val,
				i,
				[10]byte{byte(i)},
			)
		}
	}
}

func TestCreate(t *testing.T) {
	kv := KvStoreStub{}

	dir, err := ioutil.TempDir(helper.WorkingDirectory, "kv_store_")
	defer os.RemoveAll(dir)
	if err != nil {
		t.Fatalf("Unable to create temporary working directory: %v", err)
	}

	err = kv.Create(
		KvStoreConfig{
			memorySize:       100_000_000,
			workingDirectory: dir,
		},
	)
	if err != nil {
		t.Errorf("Unable to create new KV store: %v", err)
	}
}

func TestOpenAndClose(t *testing.T) {
	t.Skip("Skipping expected-failing test")
	// Open and close will be tested together as well, since one cannot be
	// tested without the other.

	kv, dir := helper.GetEmptyInstance()

	// We'll add an entry, close the KV store then reopen it and ensure
	// it's still present.
	err := kv.Put(1, [10]byte{42})
	if err != nil {
		t.Fatalf("Error putting element 1: %v", err)
	}

	err = kv.Close()
	if err != nil {
		t.Fatalf("Error closing KV store: %v", err)
	}

	err = kv.Open(dir)
	if err != nil {
		t.Fatalf("Error opening KV store: %v", err)
	}

	val, err := kv.Get(1)
	if err != nil {
		t.Fatalf("Error getting element %d: %v", 1, err)
	}

	if val != [10]byte{42} {
		t.Errorf(
			"Got unexpected value %v for key %d; expected %v",
			1,
			val,
			[10]byte{42},
		)
	}
}

func TestDelete(t *testing.T) {
	t.Skip("Skipping expected-failing test")
	kv, dir := helper.GetEmptyInstance()

	err := kv.Delete()
	if err != nil {
		t.Fatalf("Error deleting KV store: %v", err)
	}

	_, err = os.Stat(dir)
	if !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("Expected KV directory %s to not exist anymore, but did.", dir)
	}

}

// TODO: Future tests which might be required, depending on functionality of open/delete/...
// - Get/Put without having opened KV store should error sanely
// - Open should probably error if one already opened. Alternatively should close existing one.
// - Close should error if none open
