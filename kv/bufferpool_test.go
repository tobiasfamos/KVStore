package kv

import (
	"testing"
)

const (
	testBufferPoolSize = 8
	testMaxDiskSize    = 128
)

func emptyBufferPool() BufferPool {
	disk := NewRAMDisk(testMaxDiskSize, testMaxDiskSize)
	eviction := NewLRUCache(testBufferPoolSize)
	return NewBufferPool(testBufferPoolSize, disk, &eviction)
}

func TestBufferPool_NewPage(t *testing.T) {
	bufferPool := emptyBufferPool()

	// test creation
	for i := 0; i < testMaxDiskSize; i++ {
		page, err := bufferPool.NewPage()
		if err != nil {
			t.Errorf("Actual NewPage err = %s, Expected == nil", err)
		}
		if page.id != PageID(i) {
			t.Errorf("Actual pageID = %d, Expected == %d", page.id, i)
		}
		if page.pinCount != 1 {
			t.Errorf("Actual pinCount = %d, Expected == 1", page.pinCount)
		}
		if page.isDirty {
			t.Errorf("Actual isDirty = true, Expected == false")
		}
		if page.data != [PageDataSize]byte{} {
			t.Errorf("NewPage data should be zeroed")
		}

		_ = bufferPool.UnpinPage(page.id, false)
	}

	// test unable to allocate page on disk
	_, err := bufferPool.NewPage()
	if err == nil {
		t.Errorf("Actual NewPage err = nil, Expected == unable to allocate page on disk")
	}

	// test unable to reserve buffer frame
	for i := 0; i < testMaxDiskSize; i++ {
		_, _ = bufferPool.FetchPage(PageID(i))
	}
	_, err = bufferPool.NewPage()
	if err == nil {
		t.Errorf("Actual NewPage err = nil, Expected == unable to reserve buffer frame")
	}
}

func TestBufferPool_FetchPage(t *testing.T) {
	bufferPool := emptyBufferPool()

	for i := 0; i < testMaxDiskSize; i++ {
		page, _ := bufferPool.NewPage()

		// try fetch all allocated pages
		for j := 0; j <= i; j++ {
			fetch, err := bufferPool.FetchPage(PageID(j))
			if err != nil {
				t.Errorf("Actual FetchPage err = %s, Expected == nil", err)
			}
			if fetch.id != PageID(j) {
				t.Errorf("Actual FetchPage ID = %d, Expected == %d", fetch.id, j)
			}
			if i == j && fetch != page {
				t.Errorf("Actual FetchPage = %x, Expected == %x", &fetch, &page)
			}

			_ = bufferPool.UnpinPage(fetch.id, false)
		}

		_ = bufferPool.UnpinPage(page.id, false)
	}
}

func TestBufferPool_FlushPage(t *testing.T) {
	bufferPool := emptyBufferPool()

	// write page data
	page, _ := bufferPool.NewPage()
	page.data[0] = 1

	// test flushing successful
	err := bufferPool.FlushPage(page.id)
	if err != nil {
		t.Errorf("Actual FlushPage err = %s, Expected == nil", err)
	}
	read, _ := bufferPool.disk.ReadPage(page.id)
	if read.data[0] != 1 {
		t.Errorf("Actual page data[0] = %d, Expected == 0", read.data[0])
	}
}

func TestBufferPool_FlushAllPages(t *testing.T) {
	bufferPool := emptyBufferPool()

	// create data
	for i := 0; i < testBufferPoolSize; i++ {
		page, _ := bufferPool.NewPage()
		page.data[0] = byte(i)
		_ = bufferPool.UnpinPage(page.id, true)
	}

	// test flushing errors
	errs := bufferPool.FlushAllPages()
	for _, err := range errs {
		t.Logf("Actual FlushAllPages err = %s, Expected == nil", err)
	}
	if len(errs) > 0 {
		t.Errorf("Actual FLushAllPages errs = %d, Expected == 0", len(errs))
	}

	// test successful flushing
	for i := 0; i < testBufferPoolSize; i++ {
		read, _ := bufferPool.disk.ReadPage(PageID(i))
		if read.data[0] != byte(i) {
			t.Errorf("Actual page data[0] = %d, Expected == %d", read.data[0], byte(i))
		}
		_ = bufferPool.UnpinPage(read.id, false)
	}
}

func TestBufferPool_DeletePage(t *testing.T) {
	bufferPool := emptyBufferPool()

	// test delete
	for i := 0; i < testMaxDiskSize; i++ {
		for j := 0; j < i; j++ {
			page, _ := bufferPool.NewPage()
			_ = bufferPool.UnpinPage(page.id, false)

			err := bufferPool.DeletePage(page.id)
			if err != nil {
				t.Errorf("Actual DeletePage err = %s, Expected == nil", err)
			}

			_, err = bufferPool.FetchPage(page.id)
			if err == nil {
				t.Errorf("Actual FetchPage after DeletePage err = nil, Expected == page not found")
			}
		}
	}
}

func TestBufferPool_UnpinAndDeletePage(t *testing.T) {
	bufferPool := emptyBufferPool()

	// test unpin and deletion
	for i := 0; i < testMaxDiskSize; i++ {
		for j := 0; j < i; j++ {
			page, _ := bufferPool.NewPage()

			err := bufferPool.UnpinAndDeletePage(page.id)
			if err != nil {
				t.Errorf("Actual UnpinAndDeletePage err = %s, Expected == nil", err)
			}

			_, err = bufferPool.FetchPage(page.id)
			if err == nil {
				t.Errorf("Actual FetchPage after UnpinAndDeletePage err = nil, Expected == page not found")
			}
		}
	}
}

func TestBufferPool_UnpinPage(t *testing.T) {
	// TODO: Implement and test UnpinAndFlushPage as well.
}
