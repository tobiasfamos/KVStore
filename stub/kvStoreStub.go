package stub

import (
	"fmt"
	"github.com/tobiasfamos/KVStore/kv"
)

type KvStoreStub struct {
}

func (KvStoreStub) Put(key uint64, value [10]byte) {
	fmt.Printf("Add at key %d value %s", a1, a2)
}

func (KvStoreStub) Get(a1 uint64) [10]byte {
	return [10]byte{10, 10, 1}
}

func (KvStoreStub) Open(path string) error {
  return nil
}

func (KvStoreStub) Create(config kv.KvStoreConfig) error {
  return nil
}

func (KvStoreStub) Delete(path string) error {
  return nil

}
func (KvStoreStub) Close() error {
  return nil

}
