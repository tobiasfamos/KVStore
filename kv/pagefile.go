package kv

import (
	"encoding/binary"
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

// WritePage writes the page to the file.
//
// If an IO error is encountered or the file is full, an error is returned.
func (pf *PageFile) WritePage(page *Page) error {
	return nil
}

// ReadPage read the page with the given ID from the file.
//
// If an IO error is encountered or no such page exists, an error is returned.
func (pf *PageFile) ReadPage(id PageID) (*Page, error) {
	return &Page{}, nil
}

// Initialize initializes a new page file.
//
// Be mindful that this will overwrite any content of the page file, if one
// exists already.
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

	pf.storeMetaData()

	return nil
}

// Full returns a boolean indicating whether this file is full.
func (pf *PageFile) Full() bool {
	return pf.PageCount == pf.Capacity
}

func (pf *PageFile) metaDataSize() uint32 {
	// - 4 bytes for capacity
	// - 4 bytes for page count
	// - Capacity * (4+4) bytes for the map
	// - 4 bytes for CRC
	size := 4 + 4 + pf.Capacity*(4+4) + 4

	return size
}

// encodeMetaData() encodes meta data as a byte slice.
func (pf *PageFile) encodeMetaData() []byte {
	data := make([]byte, PageSize)

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
func (pf *PageFile) loadMetaData() error {
	data, err := os.ReadFile(pf.Path)
	if err != nil {
		return fmt.Errorf("IO error while trying to read meta data: %v", err)
	}

	return pf.decodeMetaData(data)
}

// storeMetaData stores the PageFile's meta data to file.
func (pf *PageFile) storeMetaData() error {
	metaData := pf.encodeMetaData()

	file, err := os.OpenFile(pf.Path, os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		return fmt.Errorf("IO error while trying to open page file: %v", err)
	}
	defer file.Close()

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
