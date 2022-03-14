package kv

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	stub "github.com/tobiasfamos/KVStore/stub"
	"testing"
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

func TestKVStoreStub(t *testing.T) {
	kvStub := new(stub.KvStoreStub)
	fmt.Println(kvStub.Get(12))
	kvStub.Put(12, [10]byte{1})
	assert.Equal(t, true, true)
}
