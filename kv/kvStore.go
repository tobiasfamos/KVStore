package kv

import (
	"errors"
	"fmt"
)

const MaxMem = 1 << (10 * 3) // 1 GB
const defPath = "."          // create in local directory

type KeyValueStore interface {
	Put(key uint64, value [10]byte)
	Get(key uint64) [10]byte
	Create(config KvStoreConfig)
	Open(path string)
	Delete(path string)
	Close()
}

type KvStoreConfig struct {
	memorySize       int
	workingDirectory string
}

func NewKvStoreInstance(size int, path string) (*KeyValueStore, error) {
	if size > MaxMem || size == 0 {
		return nil, errors.New("'size' is out of range")
	}
	if len(path) == 0 {
		return nil, errors.New("'path' is not valid")
	}
	return nil, nil
}

type KvStoreStub struct {
}

func (KvStoreStub) Put(key uint64, value [10]byte) {
	fmt.Printf("Add at key %d value %s", key, value)
}

func (KvStoreStub) Get(a1 uint64) [10]byte {
	return [10]byte{10, 10, 1}
}

func (KvStoreStub) Open(path string) error {
	return nil
}

func (KvStoreStub) Create(config KvStoreConfig) error {
	return nil
}

func (KvStoreStub) Delete(path string) error {
	return nil

}
func (KvStoreStub) Close() error {
	return nil

}
