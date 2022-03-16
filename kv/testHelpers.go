package kv

import (
	"io/ioutil"
	"log"
	"os"
)

// TestHelper provides a way to get an arbitrary amount of KV stores without
// having to worry about cleaning up their persistence layer.
type TestHelper struct {
	WorkingDirectory string
}

func (helper *TestHelper) Initialize() {
	dir, err := ioutil.TempDir("/tmp", "kv_store_testrun_")
	if err != nil {
		log.Fatalf("Unable to create temporary working directory: %v", err)
	}

	log.Printf("Using working directory for KV stores: %s", dir)
	helper.WorkingDirectory = dir
}

func (helper *TestHelper) Cleanup() {
	log.Printf("Cleaning up working directory of KV stores in %s", helper.WorkingDirectory)
	err := os.RemoveAll(helper.WorkingDirectory)

	if err != nil {
		log.Fatalf("Unable to clean up working directory: %v", err)
	}
}

// GetEmptyInstance provides a new ready-to-use KV store.
func (helper *TestHelper) GetEmptyInstance() KeyValueStore {
	return helper.GetEmptyInstanceWithMemoryLimit(100_000_000) // 1 MB
}

// GetEmptyInstanceWithMemoryLimit provides a new ready-to-use KV store with a
// custom memory limit.
func (helper *TestHelper) GetEmptyInstanceWithMemoryLimit(memoryLimit int) KeyValueStore {
	kv := KvStoreStub{}

	dir, err := ioutil.TempDir(helper.WorkingDirectory, "kv_store_")
	if err != nil {
		log.Fatalf("Unable to create temporary working directory: %v", err)
	}

	err = kv.Create(
		KvStoreConfig{
			memorySize:       memoryLimit,
			workingDirectory: dir,
		},
	)
	if err != nil {
		log.Fatalf("Unable to initialize KV store: %v", err)
	}

	return kv
}
