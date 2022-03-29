package kv

import (
	"errors"
)

/*
RAMDisk is a memory mock of a disk.
*/
type RAMDisk struct {
	maxPagesOnDisk uint32
	nextPageID     PageID
	deallocated    []PageID
	pages          map[PageID]*Page
}

func NewRAMDisk(initialSize uint32, maxPagesOnDisk uint32) Disk {
	return &RAMDisk{
		maxPagesOnDisk: maxPagesOnDisk,
		nextPageID:     0,
		deallocated:    make([]PageID, 0, 8),
		pages:          make(map[PageID]*Page, initialSize),
	}
}

func (r *RAMDisk) AllocatePage() (*Page, error) {
	page := &Page{}
	// re-allocate deallocated pages
	if len(r.deallocated) > 0 {
		page.id = r.deallocated[0]
		r.deallocated = r.deallocated[1:]
	} else if uint32(r.nextPageID) >= r.maxPagesOnDisk {
		return nil, errors.New("unable to allocate page on RAM disk")
	} else {
		page.id = r.nextPageID
		r.nextPageID++
	}
	r.pages[page.id] = page

	return page, nil
}

func (r *RAMDisk) DeallocatePage(id PageID) {
	delete(r.pages, id)
	if id < r.nextPageID {
		r.deallocated = append(r.deallocated, id)
	}
}

func (r *RAMDisk) ReadPage(id PageID) (*Page, error) {
	if page, ok := r.pages[id]; ok {
		return page, nil
	}

	return nil, errors.New("page not found")
}

func (r *RAMDisk) WritePage(page *Page) error {
	r.pages[page.id] = page

	return nil
}

func (r *RAMDisk) Occupied() uint32 {
	return uint32(len(r.pages))
}

func (r *RAMDisk) Capacity() uint32 {
	return r.maxPagesOnDisk
}
