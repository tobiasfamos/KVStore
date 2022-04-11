package kv

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
)

// PageFile represents a file on disk containing multiples pages.
//
// PageFile must be initialized before it can be written to or read from. See
// the Initialize() method for how to do so.
type PageFile struct {
	// Path is the path to the page file's location on disk.
	Path string
	// Capacity is the total number of pages this file can fit.
	Capacity uint32
	// PageCount is the number of pages currently stored in this file.
	PageCount uint32
	// PageLocations is the offset in bytes where the page with the given ID starts in the file.
	PageLocations map[PageID]uint32
}

// DeallocatePage deallocates the page with the passed ID.
//
// The page is removed from the page file's meta data, as well as zerod on
// disk. This means a deallocation will incur a write of two PageDataSize'd
// blocks.
//
// If the page is not present in the page file or an IO error is encountered,
// an error is returned.
func (pf *PageFile) DeallocatePage(id PageID) error {
	offset, exist := pf.PageLocations[id]
	if !exist {
		return fmt.Errorf("No page with ID %d in this page file", id)
	}

	// Zero page
	file, err := os.OpenFile(pf.Path, os.O_WRONLY, 0666)
	if err != nil {
		return fmt.Errorf("IO error while trying to open page file: %v", err)
	}
	defer file.Close()

	emptyPage := make([]byte, PageDataSize)
	_, err = file.WriteAt(emptyPage, int64(offset))
	if err != nil {
		return fmt.Errorf("IO error while trying to write to page file: %v", err)
	}

	delete(pf.PageLocations, id)
	pf.PageCount--
	// Persist meta data as we changed the lookup map
	err = pf.storeMetaData()
	if err != nil {
		return err
	}

	return nil
}

// WritePage writes the page to the file.
//
// If an IO error is encountered or the file is full, an error is returned.
func (pf *PageFile) WritePage(page *Page) error {
	var offset uint32
	metaDataDirty := false // Whether we must flush the PageFile's meta data

	_, exist := pf.PageLocations[page.id]
	if exist {
		// Page ID already present in file, we'll overwrite
		offset = pf.PageLocations[page.id]
	} else {
		var err error
		offset, err = pf.findEmptyOffset()
		if err != nil {
			// Page file is full
			return err
		}

		// Page ID new to this file, we'll add to the lookup map and mark our meta data as dirty
		metaDataDirty = true
		pf.PageLocations[page.id] = offset
		pf.PageCount++
	}

	data := make([]byte, PageSize)

	// The page contains 7 bytes which we needn't store here, as it's
	// either available in another place (i.e. the ID), or irrelevant (i.e.
	// the dirty flag and pin count).
	// Instead, we'll use four of these bytes to store a CRC32 checksum,
	// and then will with the actual page data.

	// Four bytes of checksum
	checksum := crc32.ChecksumIEEE(page.data[:])
	binary.BigEndian.PutUint32(data[0:4], checksum)

	// Then the PageDataSize bytes of page data
	copy(data[4:PageDataSize+4], page.data[:])

	file, err := os.OpenFile(pf.Path, os.O_WRONLY, 0666)
	if err != nil {
		return fmt.Errorf("IO error while trying to open page file: %v", err)
	}
	defer file.Close()

	_, err = file.WriteAt(data, int64(offset))
	if err != nil {
		return fmt.Errorf("IO error while trying to write to page file: %v", err)
	}

	if metaDataDirty {
		err = pf.storeMetaData()
		if err != nil {
			return err
		}
	}

	return nil
}

// findEmptyOffset finds the first unused offset in the page file.
//
// If the page file is full, an error is returned.
func (pf *PageFile) findEmptyOffset() (uint32, error) {
	var offset uint32

	if pf.Full() {
		return offset, fmt.Errorf("No space left in this page file")
	}

	// This is a very brute-force approach, but given:
	// - The lower number of pages per page file
	// - The frequency at which a new page will be written
	// This seems ok as an MVP.
	occupied := make(map[uint32]bool)
	for _, v := range pf.PageLocations {
		occupied[v] = true
	}

	// First page is for meta data, so the lowest allowed byte offset for
	// an actual page is PageSize
	// <= Because we want to support pf.Capacity *pages*, not counting the
	// one used for meta data.
	for i := uint32(PageSize); i <= pf.Capacity*PageSize; i += PageSize {
		_, exist := occupied[i]
		if !exist {
			return i, nil
		}
	}

	// We earlier ensured that we are not full, so we may never get to
	// here.
	panic(fmt.Sprintf(
		"PageFile.findEmptyOffset(): Unreachable code. Page location map: %+v",
		pf.PageLocations,
	))
}

// ReadPage read the page with the given ID from the file.
//
// If an IO error is encountered or no such page exists, an error is returned.
func (pf *PageFile) ReadPage(id PageID) (*Page, error) {
	offset, exist := pf.PageLocations[id]
	if !exist {
		return &Page{}, fmt.Errorf("No page with ID %d in this page file", id)
	}

	file, err := os.Open(pf.Path)
	if err != nil {
		return &Page{}, fmt.Errorf("Error reading page file: %v", err)
	}
	defer file.Close()

	page := make([]byte, PageSize)
	_, err = file.ReadAt(page, int64(offset))
	if err != nil {
		return &Page{}, fmt.Errorf("Error reading from page file: %v", err)
	}

	// First four bytes are checksum
	checksum := binary.BigEndian.Uint32(page[0:4])
	// Then PageDataSize of page data
	var pageData [PageDataSize]byte
	copy(pageData[:], page[4:4+PageDataSize])

	// Verify the checksum
	newChecksum := crc32.ChecksumIEEE(pageData[:])
	if newChecksum != checksum {
		return &Page{}, fmt.Errorf("Checksum in file different from checksum calculated from data: %x != %x", checksum, newChecksum)
	}

	return &Page{
		id:   id,
		data: pageData,
	}, nil
}

// Initialize initializes a new page file.
//
// If the page file already exists on disk, metadata is read from there.
// Otherwise, a new file is initalized and metadata writen to disk.
//
// If an IO error is encountered an error is returned.
func (pf *PageFile) Initialize() error {
	// We require that all our meta data fits in one page.
	size := pf.metaDataSize()
	if size > PageSize {
		return fmt.Errorf(
			"Page file metadata (%dB) does not fit in page (%dB)",
			size,
			PageSize,
		)
	}

	exists, err := pf.exists()
	if err != nil {
		return err
	}
	if exists {
		// Load from disk
		return pf.loadMetaData()
	} else {
		// Initialize and flush to disk
		pf.PageLocations = make(map[PageID]uint32)
		return pf.storeMetaData()
	}
}

// Full returns a boolean indicating whether this file is full.
func (pf *PageFile) Full() bool {
	return pf.PageCount == pf.Capacity
}

func (pf *PageFile) metaDataSize() int {
	// - 4 bytes for capacity
	// - 4 bytes for page count
	// - Capacity * (4+4) bytes for the map
	// - 4 bytes for CRC
	size := 4 + 4 + pf.Capacity*(4+4) + 4

	return int(size)
}

// encodeMetaData encodes meta data as a byte slice.
func (pf *PageFile) encodeMetaData() []byte {
	data := make([]byte, PageSize)

	// assert that we have a consistent internal state
	if len(pf.PageLocations) != int(pf.PageCount) {
		panic(fmt.Sprintf(
			"PageFile meta data encoding: Inconsistent state encountered.\nPage locations map has %d entries, yet page count is %d\n",
			len(pf.PageLocations),
			pf.PageCount,
		))
	}

	binary.BigEndian.PutUint32(data[0:4], pf.Capacity)
	binary.BigEndian.PutUint32(data[4:8], pf.PageCount)

	i := 0
	mapStart := 8
	for k, v := range pf.PageLocations {
		// Each key-value pair will take up 8 bytes, and the key to value offset is another 4 bytes
		keyStart := mapStart + i*8
		valueStart := mapStart + i*8 + 4

		binary.BigEndian.PutUint32(data[keyStart:keyStart+4], uint32(k))
		binary.BigEndian.PutUint32(data[valueStart:valueStart+4], v)

		i++
	}

	// Take care not to include the 4 0x00 bytes where the checksum will be
	// placed *in* the checksum.
	checksum := crc32.ChecksumIEEE(data[:PageSize-4])
	binary.BigEndian.PutUint32(data[PageSize-4:], checksum)

	return data

}

// loadMetaData loads the PageFile's meta data from file.
//
// This will overwrite any currently loaded meta data.
func (pf *PageFile) loadMetaData() error {
	file, err := os.Open(pf.Path)
	if err != nil {
		return fmt.Errorf("IO error while trying to open page file: %v", err)
	}
	defer file.Close()

	// First page's worth of data is meta data
	data := make([]byte, PageSize)
	_, err = file.ReadAt(data, 0)
	if err != nil {
		return fmt.Errorf("IO error while trying to read page file meta data: %v", err)
	}

	return pf.decodeMetaData(data)
}

// storeMetaData stores the PageFile's meta data to file.
func (pf *PageFile) storeMetaData() error {
	file, err := os.OpenFile(pf.Path, os.O_CREATE|os.O_WRONLY, 0666)
	if err != nil {
		return fmt.Errorf("IO error while trying to open page file: %v", err)
	}
	defer file.Close()

	metaData := pf.encodeMetaData()
	_, err = file.WriteAt(metaData, 0)
	if err != nil {
		return fmt.Errorf("IO error while writing meta data to page file: %v", err)
	}

	return nil
}

// decodeMetaData decodes meta data and sets the page file's meta data to it.
//
// If the provided binary data is not a valid encoding, an error is returned.
// The page file's meta data is not affected if this is the case.
func (pf *PageFile) decodeMetaData(data []byte) error {
	if len(data) < pf.metaDataSize() {
		return fmt.Errorf("Meta data had invalid size: %d (expected %d)", len(data), pf.metaDataSize())
	}

	checksum := binary.BigEndian.Uint32(data[len(data)-4:])
	data = data[:len(data)-4]

	newChecksum := crc32.ChecksumIEEE(data)
	if newChecksum != checksum {
		return fmt.Errorf("Checksum in file different from checksum calculated from data: %x != %x", checksum, newChecksum)
	}

	capacity := binary.BigEndian.Uint32(data[0:4])
	pageCount := binary.BigEndian.Uint32(data[4:8])

	pageLocations := make(map[PageID]uint32)
	mapStart := 8
	for i := 0; i < int(pageCount); i++ {
		keyStart := mapStart + i*8
		valueStart := mapStart + i*8 + 4

		pageID := PageID(binary.BigEndian.Uint32(data[keyStart : keyStart+4]))
		pageOffset := binary.BigEndian.Uint32(data[valueStart : valueStart+4])
		pageLocations[pageID] = pageOffset
	}

	// All went well, now we can set the values
	pf.Capacity = capacity
	pf.PageCount = pageCount
	pf.PageLocations = pageLocations

	return nil
}

// exists checks whether the page file already exists on disk
//
// For a freshly created page, exists will return false. However once the page
// has been initialized for the first time it should always return true.
func (pf *PageFile) exists() (bool, error) {
	file, err := os.Open(pf.Path)
	if err == nil {
		file.Close()
		return true, nil
	} else {
		if errors.Is(err, os.ErrNotExist) {
			return false, nil
		} else {
			return false, fmt.Errorf("IO error while checking whether file %s exists: %v", pf.Path, err)
		}
	}

}
