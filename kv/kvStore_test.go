package kv

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

var testHelper = NewStubTestHelper()

func TestIntfSize(t *testing.T) {

	tests := []struct {
		size int
		fail bool
	}{
		{0, false},
		{100, false},
		{MaxMem + 1, true},
	}

	for _, test := range tests {
		_, err := NewKvStoreInstance(test.size, ".")
		if (err != nil) != test.fail {
			t.Errorf("Size = %d, Expected fail == %t", test.size, test.fail)
		}
	}
}

// Todo Test Adding one Key value

// TODO Test Adding multiple key values

// Todo test key not found

// TODO Test Delete

// TODO Test Create

// TODO Test Create Working Directory

// TODO Test Create working Directory Default

// TODO Test adding existing Key

// TODO Stresstest with many Keys

// Todo test for data structure full (elements > size)

func TestKVStoreStub(t *testing.T) {
	kvStub := new(KvStoreStub)
	fmt.Println(kvStub.Get(12))
	kvStub.Put(12, [10]byte{1})
	assert.Equal(t, true, true)
}
