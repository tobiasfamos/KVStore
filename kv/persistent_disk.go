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

// diskMetaDataFile specifies the name of the file used by the persistent disk type
// to store its meta data.
const diskMetaDataFile = "disk.meta"

// diskPageFilePattern specifies the (printf-compatible) pattern which is used to
// determine the name to use for on-disk page files.
const diskPageFilePattern = "disk.pages.%d"

// pagesPerFile is the number of pages which will be written to a single file.
// One upper limit of (PageSize - 12) / 8 follows from the requirement that all
// the meta data of a page file (mostly the page ID -> offset lookup table) has
// to fit in the first page.
// For huge pages it might be sensible to set this limit lower, such that the
// amount of pages per page file do not exceed a few thousand, to keep overhead
// low, as its meta data structure is rather naive.
const pagesPerFile = (PageSize - 12) / 8

// PersistentDisk implements a disk which persists arbitrary pages to disk.
//
// Once instantiated it can be queried for pages, told to store pages, asked
// for a fresh page, and told to deallocate a previously allocated page.
//
// This type requires initialization, and as such should only be created via
// the NewPersistentDisk() function.
//
// Once all page operations are done, Close() must be called to make the disk
// persist its meta data.
//
// Pages are stored in separate files on disk, with each such page file
// containing pagesPerFile pages grouped together. Assignment of pages to page
// files happens based on pages' IDs.
// This does mean that initial allocations of sequential pages will be stored
// local to each other, but that later on, when pages have been recycled and
// page IDs of sequential data are not sequential anymore, disk access will be
// fragmented.
//
// Known limitations:
// - A page's ID determining the file it is stored in means that, once page IDs
//   are reused, sequential pages in terms of the user might not be sequential
//   in terms of the disk.
// - Deallocated pages are currently kept fully in memory in a slice. This
//   could lead to significant memory usage if a lot of pages are deallocated
//   without new ones being allocated.
// - While PersistentDisk does know about which pages are allocated, these
//   checks are delegated to the underlying PageFile. This does imply that each
//   read of a page, even if the page does not exist, will cause at least one
//   read from disk. As this is something which should not happen anyway, this
//   seems fine.
type PersistentDisk struct {
	Directory          string
	nextPageID         PageID
	deallocatedPageIDs []PageID
}

// NewPersistentDisk initializes a new persistent disk.
//
// If the supplied directory already contains pages persisted to disk -
// governed by the existence of the meta data store - then the persistent disk
// is initialized from that directory. Otherwise a new persistent disk is
// initialized in this directory.
//
// An error is returned if initialization fails.
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

// AllocatePage allocates a new unused page.
//
// The new page will be assigned the lowest unused page ID.
//
// An error is returned if page allocation fails.
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

// DeallocatePage deallocates a page.
//
// This will both delete the page from the meta data store, as well as
// physically zero its content on disk.
//
// Trying to deallocate an unallocated page will be a no-op, not having any effect.
func (d *PersistentDisk) DeallocatePage(id PageID) {
	pageFile, err := d.pageFile(id)
	if err != nil {
		// Unable to read page file, ID might be out of valid range. So
		// we won't deallocate the ID.
		return
	}

	err = pageFile.DeallocatePage(id)
	if err != nil {
		// Error here indicates that the page might not be present in
		// the page file, or it encountered an IO error. So we won't
		// deallocate the ID.
		return
	}

	// If we got to here we actually deallocated a page on disk, so we can
	// add it to our slice of IDs to be recycled.
	d.deallocatedPageIDs = append(d.deallocatedPageIDs, id)
}

// ReadPage reads the page with the specified ID from disk.
//
// If no page with this ID exists, or an IO error is encountered while reading
// the page, an error is returned.
func (d *PersistentDisk) ReadPage(id PageID) (*Page, error) {
	// Rather than checking whether the ID is valid by:
	// - Checking it is < nextPageID
	// - Checking it is not in our (unsorted) slice of deallocated IDs
	// We will simply try to read from the appropriate page file. That will
	// trigger that one to read its metadata, and then return an error if
	// the page does not exist.

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

// WritePage writes the given page to disk.
//
// The page must have previously been allocated via AllocatePage. Trying to
// write a page which has not ben allocated will return an error.
//
// An error is returned if an IO error is encountered.
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

// Occupied returns the number of currently allocated pages.
func (d *PersistentDisk) Occupied() uint {
	return uint(d.nextPageID) - uint(len(d.deallocatedPageIDs))
}

// Capacit returns the maximum number of supported pages.
func (d *PersistentDisk) Capacity() uint {
	// PageID is a uint32, we do not enforce any lower limits
	return math.MaxUint32 + 1
}

// Close flushes meta data to disk. After having called Close() it is save to
// discard the PersistentDisk value, as long as no further page operations are
// issued.
//
// An error is returned if an IO error is encountered.
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
	return filepath.Join(d.Directory, diskMetaDataFile)
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

	fileName := fmt.Sprintf(diskPageFilePattern, fileID)

	return filepath.Join(d.Directory, fileName)
}
