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

// Initialize initializes the helper.
//
// This should be called in the test suite's setup method.
func (helper *TestHelper) Initialize() {
	dir, err := os.MkdirTemp(os.TempDir(), "kv_store_testrun_")
	if err != nil {
		log.Fatalf("Unable to create temporary working directory: %v", err)
	}

	log.Printf("Using working directory for KV stores: %s", dir)
	helper.WorkingDirectory = dir
}

// Cleanup performs required cleanup operations.
//
// This should be called in the test suite's teardown method.
func (helper *TestHelper) Cleanup() {
	log.Printf("Cleaning up working directory of KV stores in %s", helper.WorkingDirectory)
	err := os.RemoveAll(helper.WorkingDirectory)

	if err != nil {
		log.Fatalf("Unable to clean up working directory: %v", err)
	}
}

// GetEmptyInstance provides a new ready-to-use KV store, with a memory limit
// of 100MB and a temporary working directory.
func (helper *TestHelper) GetEmptyInstance() (KeyValueStore, string) {
	return helper.GetEmptyInstanceWithMemoryLimit(PageSize * (InternalNodeSize + 1))
}

// GetEmptyInstanceWithMemoryLimit provides a new ready-to-use KV store with a
// custom memory limit. It also returns the working directory of the KV store.
func (helper *TestHelper) GetEmptyInstanceWithMemoryLimit(memoryLimit uint) (KeyValueStore, string) {
	kv := BTree{}

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

	return &kv, dir
}
