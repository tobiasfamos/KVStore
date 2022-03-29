package kv

import (
	"errors"
)

/*
RAMDisk is a memory mock of a disk.
*/
type RAMDisk struct {
	nextPageID  PageID
	deallocated []PageID
	pages       map[PageID]*Page
}

func NewRAMDisk(initialSize uint) RAMDisk {
	return RAMDisk{
		nextPageID:  0,
		deallocated: make([]PageID, 8),
		pages:       make(map[PageID]*Page, initialSize),
	}
}

func (r RAMDisk) AllocatePage() (*PageID, error) {
	var pageID PageID
	// re-allocate deallocated pages
	if len(r.deallocated) > 0 {
		pageID = r.deallocated[0]
		r.deallocated = r.deallocated[1:]

		return &pageID, nil
	}

	// cannot allocate more pages
	if r.nextPageID > MaxPagesOnDisk {
		return nil, errors.New("unable to allocate page on RAM disk")
	}

	pageID = r.nextPageID
	r.nextPageID++

	return &pageID, nil
}

func (r RAMDisk) DeallocatePage(id PageID) {
	delete(r.pages, id)
	if id < r.nextPageID {
		r.deallocated = append(r.deallocated, id)
	}
}

func (r RAMDisk) ReadPage(id PageID) (*Page, error) {
	if page, ok := r.pages[id]; ok {
		return page, nil
	}

	return nil, errors.New("page not found")
}

func (r RAMDisk) WritePage(page *Page) error {
	r.pages[page.id] = page

	return nil
}
