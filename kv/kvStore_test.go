package kv

import (
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
	kv := helper.GetEmptyInstance()

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

func TestPutExistingElement(t *testing.T) {
	kv := helper.GetEmptyInstance()

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
	kv := helper.GetEmptyInstance()

	_, err := kv.Get(1)
	if err == nil {
		t.Errorf("Expected error when getting nonexistant element; got none")
	}
}

func TestGetPutExceedingMemory(t *testing.T) {
	kv := helper.GetEmptyInstanceWithMemoryLimit(1000)

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

	for i := uint64(0); i < 100; i++ {
		val, err := kv.Get(i)
		if err != nil {
			t.Errorf("Error getting element %d: %v", i, err)
		}

		if val != [10]byte{byte(i)} {
			t.Errorf(
				"Got unexpected value %v for key %d; expected %v",
				val,
				i,
				[10]byte{byte(i)},
			)
		}
	}
}

func TestCreate(t *testing.T) {
}

func TestOpen(t *testing.T) {
}

func TestDelete(t *testing.T) {
}

func TestClose(t *testing.T) {
}

// TODO Test Delete

// TODO Test Create

// TODO Test Create Working Directory

// TODO Test Create working Directory Default

// TODO Test adding existing Key

// TODO Stresstest with many Keys

// Todo test for data structure full (elements > size)
