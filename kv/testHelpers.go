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

// TestOrBenchmark is an interface for types providing an interface common to
// the Test and Benchmark interfaces of the testing package.
// Only those functions which we actually used are defined in here.
type TestOrBenchmark interface {
	Fatalf(string, ...any)
	Errorf(string, ...any)
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
// sufficient to fit 1_000_000 pages
// This must be chosen large enough that none of the benchmark tests have to
// permanently swap to and from disk, as there's a lot of potential for
// improvement of performance.
// (Read: Our non-memory performance is dirt poor).
func (helper *TestHelper) GetEmptyInstance() (KeyValueStore, string) {
	return helper.GetEmptyInstanceWithMemoryLimit(PageSize * 1_000_000)
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
			MemorySize:       memoryLimit,
			WorkingDirectory: dir,
		},
	)
	if err != nil {
		log.Fatalf("Unable to initialize KV store: %v", err)
	}

	return &kv, dir
}

// GetTempDir creates a generic temporary directory within TestHelper's base directory.
//
// The supplied ID should be chosen to meaningfully identify the purpose of the directory.
//
// If creation of the temporary directory fails, the test aborts with a fatal error.
func (helper *TestHelper) GetTempDir(t TestOrBenchmark, id string) string {
	dir, err := os.MkdirTemp(
		helper.WorkingDirectory,
		id,
	)

	if err != nil {
		t.Fatalf("Test helper: Unable to create temporary directory: %v", err)
	}

	return dir
}

// GetTempFile creates a generic temporary file within TestHelper's base directory.
//
// The supplied ID should be chosen to meaningfully identify the purpose of the file.
//
// If creation of the temporary file fails, the test aborts with a fatal error.
func (helper *TestHelper) GetTempFile(t TestOrBenchmark, id string) string {
	file, err := os.CreateTemp(
		helper.WorkingDirectory,
		id,
	)

	if err != nil {
		t.Fatalf("Test helper: Unable to create temporary file: %v", err)
	}

	// We don't care about the fd, only its path
	file.Close()
	return file.Name()
}
