package kv

import (
	"fmt"
	"testing"
)

func TestNewPersistentDisk(t *testing.T) {
	dir := helper.GetTempDir(t, "persistent_disk")

	fmt.Printf("Got temporary directory: %s", dir)
}
