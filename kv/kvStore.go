package kv

import (
	"errors"
	"fmt"
)

const MaxMem = 1 << (10 * 3) // 1 GB
const defPath = "."          // create in local directory

// KeyValueStore defines the interface to be implemented by the KV store.
type KeyValueStore interface {
	// Put stores a new item with given key and value in the KV store. If
	// an item with the requested key already exists, an error is returned.
	Put(key uint64, value [10]byte) error
	// Get retrieves an item with given key rom the KV store. If no item
	// with the requested key exists, an error is returned.
	Get(key uint64) ([10]byte, error)
	// Create initializes a new instance of the KV store with the supplied
	// parameters. If creation fails, an error is returned.
	Create(config KvStoreConfig) error
	// Open opens an existing KV store from disk. If loading fails, an
	// error is returned.
	Open(path string) error
	// Delete deletes an existing KV store on disk. If deletion fails, an
	// error is returned.
	Delete(path string) error
	// Close persists the active KV store to disk and unloads it. If it
	// fails, an error is returned.
	Close() error
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

func (KvStoreStub) Put(key uint64, value [10]byte) error {
	fmt.Printf("Add at key %d value %s", key, value)
	return nil
}

func (KvStoreStub) Get(a1 uint64) ([10]byte, error) {
	return [10]byte{10, 10, 1}, nil
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
