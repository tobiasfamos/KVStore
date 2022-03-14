package stub

import (
	"fmt"
	"github.com/tobiasfamos/KVStore/kv"
)

type KvStoreStub struct {
}

func (KvStoreStub) Put(a1 uint64, a2 [10]byte) {
	fmt.Printf("Add at key %d value %s", a1, a2)
}

func (KvStoreStub) Get(a1 uint64) [10]byte {
	return [10]byte{10, 10, 1}
}

func (KvStoreStub) Open(path string) {
}

func (KvStoreStub) Create(config kv.KvStoreConfig) {
}

func (KvStoreStub) Delete(path string) {

}
func (KvStoreStub) Close() {

}