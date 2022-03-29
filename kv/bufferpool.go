package kv

import (
	"errors"
	"fmt"
)

// FrameID is the cache frame ID (index) associated with a Page.
type FrameID uint32

/*
BufferPool is a cache-like structure that buffers Pages from a Disk.
*/
type BufferPool struct {
	disk       Disk
	pages      []*Page
	pageLookup map[PageID]FrameID
	eviction   CacheEviction
	freeFrames []FrameID
}

/*
NewBufferPool creates a new buffer pool with a given size (number of pages).
*/
func NewBufferPool(size uint, disk Disk, eviction CacheEviction) BufferPool {
	freeFrames := make([]FrameID, size)
	for i := range freeFrames {
		freeFrames[i] = FrameID(i)
	}
	return BufferPool{
		disk:       disk,
		pages:      make([]*Page, size),
		pageLookup: make(map[PageID]FrameID, size),
		eviction:   eviction,
		freeFrames: freeFrames,
	}
}

/*
NewPage allocates a new page on the disk and caches it to buffer pool

This method returns an error if there are
- no free frames and no frame can be evicted from buffer, or
- the disk cannot allocate a new page.
*/
func (b *BufferPool) NewPage() (*Page, error) {
	// get next free frame or evict from cache
	frameID, err := b.getFrame()
	if err != nil {
		return nil, err
	}

	// allocate new page from disk
	page, err := b.disk.AllocatePage()
	if err != nil {
		return nil, err
	}

	page.pinCount = 1
	b.pageLookup[page.id] = *frameID
	b.pages[*frameID] = page

	return page, nil
}

/*
FetchPage fetches a page from buffer cache or disk.

This method returns ans error if there are
- no free frames and no frame can be evicted from buffer, or
- the page cannot be found in buffer or disk.
*/
func (b *BufferPool) FetchPage(pageID PageID) (*Page, error) {
	// try fetch from cache
	if frameID, ok := b.pageLookup[pageID]; ok {
		page := b.pages[frameID]
		page.pinCount++
		b.eviction.Remove(frameID)

		return page, nil
	}

	// get next free frame or evict from cache
	frameID, err := b.getFrame()
	if err != nil {
		return nil, err
	}

	// try fetch from disk
	page, err := b.disk.ReadPage(pageID)
	if err != nil {
		return nil, err
	}

	page.pinCount++
	b.pageLookup[pageID] = *frameID
	b.pages[*frameID] = page

	return page, nil
}

/*
FlushPage flushes a page to disk.
If writing to disk fails, the page gets reset and the error is returned.
If the pageID cannot be found, an error is returned.
*/
func (b *BufferPool) FlushPage(pageID PageID) error {
	if frameID, ok := b.pageLookup[pageID]; ok {
		return b.FlushFrame(frameID)
	}

	return errors.New("page not found")
}

/*
FlushFrame flushes a page associated to the frameID to disk.
If writing to disk fails, the page gets reset and the error is returned.
*/
func (b *BufferPool) FlushFrame(frameID FrameID) error {
	page := b.pages[frameID]
	wasDirty := page.isDirty
	page.isDirty = false

	if err := b.disk.WritePage(page); err != nil {
		page.isDirty = wasDirty
		return err
	}

	return nil
}

/*
FlushAllPages flushes all pages to disk.

Return an array of potential errors that happened.
*/
func (b *BufferPool) FlushAllPages() []error {
	var errs []error
	for pageID := range b.pageLookup {
		err := b.FlushPage(pageID)
		if err != nil {
			errs = append(errs, err)
		}
	}

	return errs
}

/*
DeletePage deletes a page from the buffer pool and disk.

Returns an error only if
- the page is still pinned (pinCount > 0)
- inconsistent state was detected (debugging only)
*/
func (b *BufferPool) DeletePage(pageID PageID) error {
	var frameID FrameID
	var ok bool

	// don't do anything when page is not in cache
	if frameID, ok = b.pageLookup[pageID]; !ok {
		return nil
	}

	page := b.pages[frameID]
	if page.pinCount > 0 {
		return errors.New("page cannot be deleted from buffer: pin count > 0")
	}
	if page.id != pageID {
		return fmt.Errorf("incostent state: page.id (%d) != pageID (%d)", page.id, pageID) // good to catch logic bugs
	}

	delete(b.pageLookup, pageID)
	b.eviction.Remove(frameID)
	b.disk.DeallocatePage(pageID)
	b.freeFrames = append(b.freeFrames, frameID)

	return nil
}

/*
UnpinPage unpins a page from the buffer pool for the current thread, potentially flagging the page as dirty.
If there are no more references to the page, the page is eligible for cache eviction.

Returns an error only if the page was not found.
*/
func (b *BufferPool) UnpinPage(pageID PageID, isDirty bool) error {
	if frameID, ok := b.pageLookup[pageID]; ok {
		page := b.pages[frameID]
		page.decrementPinCount()
		page.isDirty = page.isDirty || isDirty

		if page.pinCount == 0 {
			b.eviction.Add(frameID)
		}

		return nil
	}

	return errors.New("page not found")
}

/*
UnpinAndFlushPage unpins the page and flushes it to disk.
If there are no more references to the page, the page is eligible for cache eviction.

Returns an error only if the page was not found or flushing failed.
*/
func (b *BufferPool) UnpinAndFlushPage(pageID PageID) error {
	if frameID, ok := b.pageLookup[pageID]; ok {
		page := b.pages[frameID]
		page.decrementPinCount()
		wasDirty := page.isDirty
		page.isDirty = false

		if err := b.disk.WritePage(page); err != nil {
			page.isDirty = wasDirty
			return err
		}
		if page.pinCount == 0 {
			b.eviction.Add(frameID)
		}

		return nil
	}

	return errors.New("page not found")
}

/*
getFrame returns a frame.
The frame may either be from the
- free frames list, or from
- cache eviction
If evicted, the frame gets removed from cache, potentially flushing to disk if the associated page was dirty.

Returns an error if no frame can be allocated or flushing to disk fails.
*/
func (b *BufferPool) getFrame() (*FrameID, error) {
	var frameID *FrameID
	isEvicted := false

	// get next free frame or evict from cache
	if len(b.freeFrames) > 0 {
		frameID = &b.freeFrames[0]
		b.freeFrames = b.freeFrames[1:]
	} else {
		frameID = b.eviction.Victim()
		isEvicted = true
	}

	// no free frame found
	if frameID == nil {
		return nil, errors.New("unable to reserve buffer frame")
	}

	// if evicted from cache, update the table and write to disk when dirty
	if isEvicted {
		page := b.pages[*frameID]
		if page != nil {
			if page.isDirty {
				page.isDirty = false
				if err := b.disk.WritePage(page); err != nil {
					page.isDirty = true
					return nil, err
				}
			}

			delete(b.pageLookup, page.id)
		}
	}

	return frameID, nil
}
