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
		fmt.Println(path)
		return nil, errors.New("'path' is not valid")
	}
	return nil, nil
}
