package kv

import (
	"errors"
	"fmt"
)

const MaxMem = 1 << (10 * 3) // Do not allow KV stores to use more than 1GB of memory
const DefaultPath = "."      // Default to current working directory to persist KV store

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

	// Delete deletes the currently opened KV store. If deletion fails, an
	// error is returned.
	Delete() error

	// Close persists the active KV store to disk and unloads it. If it
	// fails, an error is returned.
	Close() error
}

func NewBPlusStore() *BPlusStore {
	//TODO Remove
	newCacheEviciton := NewLRUCache(12)
	newRamDisk := NewRAMDisk(120000, 12)
	store := BPlusStore{
		rootNode:   InternalNode{},
		bufferPool: NewBufferPool(12, newRamDisk, &newCacheEviciton),
	}
	return &store
}

// KVStoreConfig provides parameters used to initialize a new KV store.
type KvStoreConfig struct {
	memorySize       uint   // Maximum amount of memory to be used by KV store
	workingDirectory string // Directory on disk in which KV store will be persisted
}

func NewKvStoreInstance(size int, path string) (*KeyValueStore, error) {
	if size > MaxMem || size < 0 {
		return nil, fmt.Errorf("'size' must be between 0 and %d", MaxMem)
	}
	if len(path) == 0 {
		return nil, errors.New("'path' is not valid")
	}
	return nil, nil
}

// KvStoreStub is a stubbed implementation of the KV interface. It allows
// building the project and running the tests in the first phase.
type KvStoreStub struct {
}

func (KvStoreStub) Put(key uint64, value [10]byte) error {
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

func (KvStoreStub) Delete() error {
	return nil

}
func (KvStoreStub) Close() error {
	return nil
}
