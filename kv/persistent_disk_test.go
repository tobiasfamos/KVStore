package kv

import (
	"bytes"
	"fmt"
	"math"
	"math/rand"
	"path/filepath"
	"testing"
)

// We'll also assert correctness within this benchmark, so it's a bit of both.
// Treating it as a benchmark makes sure it won't run as part of normal unit
// tests though, which is beneficial as it is a tad slow.
func BenchmarkReadWriteDeallocateLots(b *testing.B) {
	for benchIter := 0; benchIter < b.N; benchIter++ {
		disk, dir := newDisk(b)

		// We'll write 10k pages. With a 4KiB page size that will be around 40
		// MiB, spread across 10 page files. That should trigger any bug with
		// the fanout to different files.

		pagesToWrite := 10_000

		// Expected contents of pages
		pages := make(map[PageID][]byte)

		// Allocate pages, fill them with random bytes from above, then write them to disk
		for i := 0; i < pagesToWrite; i++ {
			page, err := disk.AllocatePage()
			if err != nil {
				b.Fatalf("Error allocating page: %v", err)
			}

			// Fill with random bytes
			pages[page.id] = make([]byte, PageDataSize)
			_, err = rand.Read(pages[page.id])
			if err != nil {
				b.Fatalf("Error reading random bytes: %v", err)
			}

			copy(page.data[:], pages[page.id])

			err = disk.WritePage(page)
			if err != nil {
				b.Fatalf("Error writing page: %v", err)
			}
		}

		// Close and reopen
		err := disk.Close()
		if err != nil {
			b.Fatalf("Error closing disk: %v", err)
		}
		disk = existingDisk(b, dir)

		// First read all of them, ensure they are correct still
		for id, pageData := range pages {
			page, err := disk.ReadPage(id)
			if err != nil {
				b.Fatalf("Error reading page: %v", err)
			}

			if !bytes.Equal(pageData, page.data[:]) {
				b.Fatalf(
					"Got unexpected data for page %d. Read %x; Expected %x",
					id,
					page.data,
					pages[id],
				)
			}
		}

		// Now deallocate about 20% of pages
		deallocated := make(map[PageID]bool)
		for id := range pages {
			if rand.Intn(10) < 2 {
				deallocated[id] = true
				// No, it does not return an error value as per its interface.
				disk.DeallocatePage(id)
			}
		}

		// Close and reopen
		err = disk.Close()
		if err != nil {
			b.Fatalf("Error closing disk: %v", err)
		}
		disk = existingDisk(b, dir)

		// And allocate about 50% of the deallocated ones again
		for i := 0; i < len(deallocated); i++ {
			if rand.Intn(10) < 1 {
				page, err := disk.AllocatePage()
				if err != nil {
					b.Fatalf("Error allocating page: %v", err)
				}

				deallocated[page.id] = false

				// And fill it with new data
				_, err = rand.Read(pages[page.id])
				if err != nil {
					b.Fatalf("Error reading random bytes: %v", err)
				}

				copy(page.data[:], pages[page.id])

				// Write to disk again
				err = disk.WritePage(page)
				if err != nil {
					b.Fatalf("Error writing page: %v", err)
				}
			}
		}

		// Close and reopen
		err = disk.Close()
		if err != nil {
			b.Fatalf("Error closing disk: %v", err)
		}
		disk = existingDisk(b, dir)

		// Now read all pages one last time
		for id, pageData := range pages {
			// But skip those which were deallocated. We'll use that the
			// default value of a boolean is false, so no need to check for
			// existence of the key in the map.
			if deallocated[id] {
				continue
			}

			page, err := disk.ReadPage(id)
			if err != nil {
				b.Fatalf("Error reading page: %v", err)
			}

			if !bytes.Equal(pageData, page.data[:]) {
				b.Errorf(
					"Got unexpected data for page %d. Read %x; Expected %x",
					id,
					page.data,
					pageData,
				)
			}
		}
	}
}

func TestNewPersistentDisk(t *testing.T) {
	dir := helper.GetTempDir(t, "persistent_disk")

	disk, err := NewPersistentDisk(dir)
	if err != nil {
		t.Fatalf("Got error while creating new persistent disk: %v", err)
	}

	pdisk := disk.(*PersistentDisk)

	if pdisk.Directory != dir {
		t.Errorf("Expected disk to use directory %s; but got %s", dir, pdisk.Directory)
	}
}

func TestReadUnallocatedPage(t *testing.T) {
	disk, _ := newDisk(t)

	// Try reading unallocated page
	_, err := disk.ReadPage(42)
	if err == nil {
		t.Error("Expected error when reading unallocated page; got none")
	}

	page, err := disk.AllocatePage()
	if err != nil {
		t.Fatalf("Got error while allocating page: %v", err)
	}

	// Try reading previously deallocated page
	disk.DeallocatePage(page.id)
	_, err = disk.ReadPage(page.id)
	if err == nil {
		t.Error("Expected error when reading deallocated page; got none")
	}

}

func TestDeallocatePageWithUnallocatedPage(t *testing.T) {
	disk, _ := newDisk(t)

	// DeallocatePage, per its interface, should always be quiet.
	disk.DeallocatePage(42)
}

func TestAllocatePage(t *testing.T) {
	disk, dir := newDisk(t)

	for i := 0; i < 10; i++ {
		page, err := disk.AllocatePage()
		if err != nil {
			t.Fatalf("Got error while allocating page %d: %v", i, err)
		}

		if page.id != PageID(i) {
			t.Errorf("Expected page %d to have ID %d; got %d", i, i, page.id)
		}

		if page.isDirty {
			t.Errorf("Expected page %d to not be dirty, but was", page.id)
		}

		if len(page.data) != PageDataSize {
			t.Errorf("Expected page %d data to be of size %d; was %d", i, PageDataSize, len(page.data))
		}

		if page.data != [PageDataSize]byte{} {
			t.Errorf("Expected page data to be %d-length zero-byte array, but was %x", PageDataSize, page.data)
		}
	}

	// Ensure we get sequential IDs after a reload still
	disk.Close()
	disk = existingDisk(t, dir)

	for i := 10; i < 20; i++ {
		page, err := disk.AllocatePage()
		if err != nil {
			t.Fatalf("Got error while allocating page %d: %v", i, err)
		}

		if page.id != PageID(i) {
			t.Errorf("Expected page %d to have ID %d; got %d", i, i, page.id)
		}
	}
}

func TestAllocatePageReusesIDs(t *testing.T) {
	disk, dir := newDisk(t)

	for i := 0; i < 10; i++ {
		_, err := disk.AllocatePage()
		if err != nil {
			t.Fatalf("Got error while allocating page %d: %v", i, err)
		}
	}

	deallocatedIDs := []PageID{0, 3, 9}
	for _, id := range deallocatedIDs {
		disk.DeallocatePage(PageID(id))
	}

	// Ensure IDs reused in sequence before new ones assigned
	for _, id := range deallocatedIDs {
		page, err := disk.AllocatePage()
		if err != nil {
			t.Fatalf("Got error while allocating page: %v", err)
		}

		if page.id != PageID(id) {
			t.Errorf("Expected reused page to have ID %d; got %d", id, page.id)
		}
	}

	// Ensure we reuse IDs after a reload still
	deallocatedIDs = []PageID{2, 8, 4}
	for _, id := range deallocatedIDs {
		disk.DeallocatePage(PageID(id))
	}

	disk.Close()
	disk = existingDisk(t, dir)

	for _, id := range deallocatedIDs {
		page, err := disk.AllocatePage()
		if err != nil {
			t.Fatalf("Got error while allocating page: %v", err)
		}

		if page.id != PageID(id) {
			t.Errorf("Expected reused page to have ID %d; got %d", id, page.id)
		}
	}
}

func TestAllocatePageWritesToDisk(t *testing.T) {
	disk, _ := newDisk(t)

	for i := 0; i < 10; i++ {
		page, err := disk.AllocatePage()
		if err != nil {
			t.Fatalf("Got error while allocating page %d: %v", i, err)
		}

		pageFile, err := disk.pageFile(page.id)
		if err != nil {
			t.Fatalf("Error getting page file of page %d: %v", i, err)
		}

		readPage, err := pageFile.ReadPage(page.id)
		if err != nil {
			t.Fatalf("Error reading page %d from page file: %v", i, err)
		}

		if !page.Equal(readPage) {
			t.Errorf("Page from page file differs from allocated page: %+v != %+v", readPage, page)
		}
	}

}

func TestCapacity(t *testing.T) {
	disk, _ := newDisk(t)

	if disk.Capacity() != math.MaxUint32+1 {
		t.Errorf("Expected disk to have capacity %d; got %d", math.MaxUint32+1, disk.Capacity())
	}
}

func TestOccupied(t *testing.T) {
	disk, dir := newDisk(t)

	if disk.Occupied() != 0 {
		t.Errorf("Expected disk to have no occupied pages; got %d", disk.Occupied())
	}

	for i := 0; i < 10; i++ {
		_, err := disk.AllocatePage()
		if err != nil {
			t.Fatalf("Error allocating page: %v", err)
		}
	}

	if disk.Occupied() != 10 {
		t.Errorf("Expected disk to have 10 occupied pages; got %d", disk.Occupied())
	}

	for i := 0; i < 4; i++ {
		disk.DeallocatePage(PageID(i))
	}

	if disk.Occupied() != 6 {
		t.Errorf("Expected disk to have 6 occupied pages; got %d", disk.Occupied())
	}

	// Lastly check that it survives a close/load cycle
	disk.Close()
	disk = existingDisk(t, dir)

	if disk.Occupied() != 6 {
		t.Errorf("Expected disk to have 6 occupied pages after close/load cycle; got %d", disk.Occupied())
	}
}

func TestEncodeMetaData(t *testing.T) {
	disk := PersistentDisk{
		nextPageID: 1074701930,
		deallocatedPageIDs: []PageID{
			0, 1, 42, 257, 3120, 22222, 1073470479,
		},
	}

	metaData := []byte{
		0x40, 0x0e, 0xa6, 0x6a, // nextPageID

		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x07, // len(deallocatedPageIDs)
		0x00, 0x00, 0x00, 0x00, // 0
		0x00, 0x00, 0x00, 0x01, // 1
		0x00, 0x00, 0x00, 0x2a, // 42
		0x00, 0x00, 0x01, 0x01, // 257
		0x00, 0x00, 0x0c, 0x30, // 3120
		0x00, 0x00, 0x56, 0xce, // 22222,
		0x3f, 0xfb, 0xdc, 0x0f, // 1073470479

		0xdb, 0x7b, 0x57, 0xf8, // Checksum
	}

	actual := disk.encodeMetaData()
	if !bytes.Equal(metaData, actual) {
		t.Errorf("Got unexpected encoding of metadata %x; expected %x", actual, metaData)
	}
}

func TestDecodeMetaData(t *testing.T) {
	disk := PersistentDisk{}

	metaData := []byte{
		0x40, 0x0e, 0xa6, 0x6a, // 1074701930

		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x07, // len(deallocatedPageIDs)
		0x00, 0x00, 0x00, 0x00, // 0
		0x00, 0x00, 0x00, 0x01, // 1
		0x00, 0x00, 0x00, 0x2a, // 42
		0x00, 0x00, 0x01, 0x01, // 257
		0x00, 0x00, 0x0c, 0x30, // 3120
		0x00, 0x00, 0x56, 0xce, // 22222,
		0x3f, 0xfb, 0xdc, 0x0f, // 1073470479

		0xdb, 0x7b, 0x57, 0xf8, // Checksum
	}

	err := disk.decodeMetaData(metaData)
	if err != nil {
		t.Fatalf("Error decoding meta data: %v", err)
	}

	if disk.nextPageID != 1074701930 {
		t.Errorf("Got next page ID %d; expected %d", disk.nextPageID, 1074701930)
	}

	ids := []PageID{0, 1, 42, 257, 3120, 22222, 1073470479}
	if len(ids) != len(disk.deallocatedPageIDs) {
		t.Fatalf("Got unexpected number of deallocated page IDs %d; expected %d", len(disk.deallocatedPageIDs), len(ids))
	}
	for i, id := range ids {
		if id != disk.deallocatedPageIDs[i] {
			t.Errorf(
				"Got unexpected deallocatd page ID at index %d: %d; expected %d",
				i,
				disk.deallocatedPageIDs[i],
				id,
			)
		}
	}
}

func TestDecodeMetaDataWithInvalidData(t *testing.T) {
	pageIDs := []PageID{
		1, 2, 20, 30, 42, 202,
	}
	disk := PersistentDisk{
		Directory:          "foo/bar",
		nextPageID:         123,
		deallocatedPageIDs: pageIDs,
	}

	err := disk.decodeMetaData([]byte{0x00, 0x00, 0x02, 0x04, 0x08, 0x10})
	if err == nil {
		t.Fatalf("Expected error when setting invalid meta data; got none")
	}

	if disk.Directory != "foo/bar" {
		t.Errorf(
			"Expected disk's next page ID not to be affected; but is now %s",
			disk.Directory,
		)
	}

	if disk.nextPageID != 123 {
		t.Errorf(
			"Expected disk's next page ID not to be affected; but is now %d",
			disk.nextPageID,
		)
	}

	for i := 0; i < len(pageIDs); i++ {
		if pageIDs[i] != disk.deallocatedPageIDs[i] {
			t.Errorf(
				"Expected disk's deallocated page IDs to not be affected, but ID %d is now %d",
				i,
				disk.deallocatedPageIDs[i],
			)
		}
	}

}

func TestPageFilePath(t *testing.T) {
	disk := PersistentDisk{Directory: "foo"}

	pageIDs := []PageID{0, 2, 999, 1000, 4242}

	for _, pageID := range pageIDs {
		fileID := pageID / pagesPerFile
		fileName := disk.pageFilePath(pageID)
		expectedFileName := filepath.Join(
			"foo",
			fmt.Sprintf(diskPageFilePattern, fileID),
		)

		if fileName != expectedFileName {
			t.Errorf(
				"Expected page %d to yield page file %s; got %s",
				pageID,
				expectedFileName,
				fileName,
			)
		}
	}
}

func newDisk(t Fatalfer) (*PersistentDisk, string) {
	dir := helper.GetTempDir(t, "persistent_disk")

	disk, err := NewPersistentDisk(dir)
	if err != nil {
		t.Fatalf("Got error while creating new persistent disk: %v", err)
	}

	pdisk := disk.(*PersistentDisk)

	return pdisk, dir
}

func existingDisk(t Fatalfer, dir string) *PersistentDisk {
	disk, err := NewPersistentDisk(dir)
	if err != nil {
		t.Fatalf("Got error while loading existing persistent disk: %v", err)
	}

	pdisk := disk.(*PersistentDisk)

	return pdisk
}
