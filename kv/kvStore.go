package kv

import (
	"errors"
	"fmt"
)

const MaxMem = 1 << (10 * 3) // 1 GB
const defPath = "."          // create in local directory

type kvStore interface {
	put(a1 uint64, a2 [10]byte)
	get(a1 uint64) [10]byte
}

type kvStoreControl interface {
	open()
	delete()
	close()
}

type kvStoreStub struct {
}

func (kvStoreStub) put(a1 uint64, a2 [10]byte) {
	fmt.Printf("Add at key %d value %s", a1, a2)
}

func (kvStoreStub) get(a1 int) [10]byte {
	return [10]byte{10, 10, 1}
}

func NewKvStoreInstance(size int, path string) (*kvStore, error) {
	if size > MaxMem || size == 0 {
		return nil, errors.New("'size' is out of range")
	}
	if len(path) == 0 {
		fmt.Println(path)
		return nil, errors.New("'path' is not valid")
	}
	return nil, nil
}
