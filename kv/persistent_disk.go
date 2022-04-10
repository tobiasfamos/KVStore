package kv

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"math"
	"os"
	"path/filepath"
)

const metaDataFile = "btree.meta"
const pageFilePattern = "btree.pages.%d"

// pagesPerFile is the number of pages which will be written to a single file.
// One upper limit of (PageSize - 12) / 8 follows from the requirement that all
// the meta data of a page file (mostly the page ID -> offset lookup table) has
// to fit in the first page.
// For huge pages it might be sensible to set this limit lower, such that the
// amount of pages per page file do not exceed a few thousand, to keep overhead
// low, as its meta data structure is rather naive.
const pagesPerFile = (PageSize - 12) / 8

type PersistentDisk struct {
	Directory          string
	nextPageID         PageID
	deallocatedPageIDs []PageID
}

func NewPersistentDisk(directory string) (Disk, error) {
	d := &PersistentDisk{
		Directory: directory,
	}

	d.deallocatedPageIDs = make([]PageID, 0)

	err := d.initialize()

	return d, err
}

func (d *PersistentDisk) initialize() error {
	file, err := os.Open(d.metaFilePath())
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			// Initializing new store in this directory.
			// Currently this only involves us dumping our current meta data to disk.
			return d.storeMetaData()

		} else {
			return fmt.Errorf("Unexpected IO error while checking existence of meta data file: %v", err)
		}
	}

	// File exists, so there's already a store present in this directory.
	// Close file, and load meta data from disk.
	file.Close()
	return d.loadMetaData()
}

func (d *PersistentDisk) AllocatePage() (*Page, error) {
	var id PageID

	if len(d.deallocatedPageIDs) == 0 {
		id = d.nextPageID
		d.nextPageID++
	} else {
		id = d.deallocatedPageIDs[0]
		d.deallocatedPageIDs = d.deallocatedPageIDs[1:]
	}

	p := Page{
		id: id,
	}

	// We'll write freshly allocated pages to disk. This is required, as:
	// - They might end up in a new file which does not exist yet
	// - They might end up in a new part of a file which wasn't allocated yet
	// While it would not be required for recycled pages, as those will
	// have been zeroed, quickly writing them doesn't hurt us a lot.
	err := d.WritePage(&p)

	return &p, err
}

func (d *PersistentDisk) DeallocatePage(id PageID) {
	d.deallocatedPageIDs = append(d.deallocatedPageIDs, id)
}

func (d *PersistentDisk) ReadPage(id PageID) (*Page, error) {
	pageFile, err := d.pageFile(id)
	if err != nil {
		return &Page{}, err
	}

	page, err := pageFile.ReadPage(id)
	if err != nil {
		return &Page{}, err
	}

	return page, nil
}

func (d *PersistentDisk) WritePage(page *Page) error {
	pageFile, err := d.pageFile(page.id)
	if err != nil {
		return err
	}

	err = pageFile.WritePage(page)
	if err != nil {
		return err
	}

	return nil
}

func (d *PersistentDisk) Occupied() uint {
	return uint(d.nextPageID) - uint(len(d.deallocatedPageIDs))
}

func (d *PersistentDisk) Capacity() uint {
	// PageID is a uint32, we do not enforce any lower limits
	return math.MaxUint32 + 1
}

func (d *PersistentDisk) Close() error {
	err := d.storeMetaData()

	return err
}

// loadMetaData loads the disk's meta data to file.
func (d *PersistentDisk) loadMetaData() error {
	data, err := os.ReadFile(d.metaFilePath())
	if err != nil {
		return fmt.Errorf("IO error while trying to read meta data: %v", err)
	}

	return d.decodeMetaData(data)
}

// storeMetaData stores the disk's meta data to file.
func (d *PersistentDisk) storeMetaData() error {
	metaData := d.encodeMetaData()

	err := os.WriteFile(d.metaFilePath(), metaData, 0660)
	if err != nil {
		return fmt.Errorf("IO error while trying to write meta data: %v", err)
	}

	return nil
}

// encodeMetaData encodes the disk's meta data into a byte slice.
func (d *PersistentDisk) encodeMetaData() []byte {
	// 4 bytes for nextPageID
	// 8 bytes for length of deallocatedPageIDs
	// 4 bytes for each entry in deallocatedPageIDs
	// 4 bytes checksum
	dataLength := 4 + 8 + len(d.deallocatedPageIDs)*4 + 4
	data := make([]byte, dataLength)

	binary.BigEndian.PutUint32(data[0:4], uint32(d.nextPageID))
	binary.BigEndian.PutUint64(data[4:12], uint64(len(d.deallocatedPageIDs)))
	for i, id := range d.deallocatedPageIDs {
		binary.BigEndian.PutUint32(data[12+i*4:12+(i+1)*4], uint32(id))
	}

	// Take care not to include the 4 0x00 bytes where the checksum will be
	// placed *in* the checksum.
	checksum := crc32.ChecksumIEEE(data[:dataLength-4])
	binary.BigEndian.PutUint32(data[dataLength-4:], checksum)

	return data
}

// decodeMetaData decodes meta data and sets the disks's meta data to it.
//
// If the provided binary data is not a valid encoding, an error is returned.
// The disk's meta data is not affected if this is the case.
func (d *PersistentDisk) decodeMetaData(data []byte) error {
	checksum := binary.BigEndian.Uint32(data[len(data)-4:])
	data = data[:len(data)-4]

	newChecksum := crc32.ChecksumIEEE(data)
	if newChecksum != checksum {
		return fmt.Errorf("Checksum in file different from checksum calculated from data: %x != %x", checksum, newChecksum)
	}

	nextPageID := PageID(binary.BigEndian.Uint32(data[0:4]))
	deallocatedPageCount := binary.BigEndian.Uint64(data[4:12])
	deallocatedPageIDs := make([]PageID, deallocatedPageCount)
	for i := 0; i < int(deallocatedPageCount); i++ {
		deallocatedPageIDs[i] = PageID(binary.BigEndian.Uint32(
			data[12+i*4 : 12+(i+1)*4],
		))
	}

	// Now we were able to load it all, so we can overwrite it
	d.nextPageID = nextPageID
	d.deallocatedPageIDs = deallocatedPageIDs

	return nil
}

// metaFilePath returns the file path of the file containing the meta data.
func (d *PersistentDisk) metaFilePath() string {
	return filepath.Join(d.Directory, metaDataFile)
}

// pageFile returns a PageFile containing the requested page.
func (d *PersistentDisk) pageFile(id PageID) (*PageFile, error) {
	path := d.pageFilePath(id)

	pageFile := PageFile{
		Path:     path,
		Capacity: pagesPerFile,
	}
	err := pageFile.Initialize()

	return &pageFile, err
}

// pageFilePath returns the file path of the file containing the given page.
func (d *PersistentDisk) pageFilePath(id PageID) string {
	// Assuming e.g. 1000 pages per file, then pages 0 through 999 are
	// stored in file 0, 1000 through 1999 in file 1, etc.
	fileID := id / pagesPerFile

	fileName := fmt.Sprintf(pageFilePattern, fileID)

	return filepath.Join(d.Directory, fileName)
}
