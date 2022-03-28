package kv

const MaxPagesOnDisk = 65535

type Disk interface {
	/*
		AllocatePage allocates a new page and returns the associated ID.

		Returns nil if no page can be allocated.
	*/
	AllocatePage() *PageID
	// DeallocatePage deallocates a page.
	DeallocatePage(PageID)
	// ReadPage reads a page if present. Otherwise an error will be raised.
	ReadPage(PageID) (*Page, error)
	// WritePage writes/updates a page.
	WritePage(*Page) error
}
