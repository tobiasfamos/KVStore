package kv

type Disk interface {
	/*
		AllocatePage allocates a new page and returns the associated ID.
		Returns an error if no page can be allocated.
	*/
	AllocatePage() (*Page, error)
	// DeallocatePage deallocates a page.
	DeallocatePage(PageID)
	// ReadPage reads a page if present. Otherwise an error will be raised.
	ReadPage(PageID) (*Page, error)
	// WritePage writes/updates a page.
	WritePage(*Page) error
	// Occupied returns the number of occupied pages.
	Occupied() uint
	// Total capacity of this disk.
	Capacity() uint
	// Close closes the disk, performing all required cleanup for a clean shutdown.
	// After a close, the disk must not be used anymore.
	Close() error
}
