package kv

import (
	"errors"
	"fmt"
)

// FrameID is the cache frame ID (index) associated with a Page.
type FrameID uint

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

func (b *BufferPool) GetDebugInfo() string {
	debug := fmt.Sprintf("%T {"+
		"\n\tdisk:              %T"+
		"\n\teviction:          %T"+
		"\n\t# of pages:        %d"+
		"\n\t# of cached pages: %d"+
		"\n\tpages:\n",
		b, b.disk, b.eviction, len(b.pages), len(b.pageLookup),
	)

	for _, page := range b.pages {
		if page == nil {
			continue
		}

		if _, ok := b.pageLookup[page.id]; ok {
			lnode, inode := RawNodeFrom(page)
			if lnode != nil {
				debug += lnode.GetDebugInfo()
			} else {
				debug += inode.GetDebugInfo()
			}
		}
	}

	return debug + "}"
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
	fh := b.getFrame()
	if fh.err != nil {
		return nil, fmt.Errorf("Error getting frame: %v", fh.err)
	}

	// allocate new page from disk
	fh.AllocatePage()
	if fh.err != nil {
		return nil, fmt.Errorf("Error allocating page on frame: %v", fh.err)
	}

	fh.page.pinCount = 1
	b.pageLookup[fh.page.id] = *fh.frameID
	b.pages[*fh.frameID] = fh.page

	return fh.page, nil
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
	fh := b.getFrame()
	if fh.err != nil {
		return nil, fh.err
	}

	// try fetch from disk
	fh.ReadPage(pageID)
	if fh.err != nil {
		return nil, fh.err
	}

	fh.page.pinCount++
	b.pageLookup[pageID] = *fh.frameID
	b.pages[*fh.frameID] = fh.page

	return fh.page, nil
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

// Close prepares flushes all pages and closes the underlying disk.
//
// Once Close() has been called, the buffer pool has persisted all data to disk
// and can thus be safely deallocated.
func (b *BufferPool) Close() error {
	errors := b.FlushAllPages()
	if len(errors) != 0 {
		return fmt.Errorf("Errors while flushing pages to disk: %v", errors)
	}

	err := b.disk.Close()
	if err != nil {
		return fmt.Errorf("Error closing disk: %v", err)
	}

	return nil
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

	// remove from buffer when in buffer
	if frameID, ok = b.pageLookup[pageID]; ok {
		page := b.pages[frameID]
		if page.pinCount > 0 {
			return errors.New("page cannot be deleted from buffer: pin count > 0")
		}
		if page.id != pageID {
			return fmt.Errorf("incostent state: page.id (%d) != pageID (%d)", page.id, pageID) // good to catch logic bugs
		}

		delete(b.pageLookup, pageID)
		b.eviction.Remove(frameID)
	}

	b.disk.DeallocatePage(pageID)
	b.freeFrames = append(b.freeFrames, frameID)

	return nil
}

/*
UnpinAndDeletePage unpins the page and deletes it from the buffer pool and disk.

Returns an error only if
- the page is still pinned from somewhere else (pinCount > 0)
- inconsistent state was detected (debugging only)
*/
func (b *BufferPool) UnpinAndDeletePage(pageID PageID) error {
	var frameID FrameID
	var ok bool

	// remove from buffer when in buffer
	if frameID, ok = b.pageLookup[pageID]; ok {
		page := b.pages[frameID]
		page.decrementPinCount()
		if page.pinCount > 0 {
			return errors.New("page cannot be deleted from buffer: pin count > 0")
		}
		if page.id != pageID {
			return fmt.Errorf("incostent state: page.id (%d) != pageID (%d)", page.id, pageID) // good to catch logic bugs
		}

		delete(b.pageLookup, pageID)
		b.eviction.Remove(frameID)
	}

	b.disk.DeallocatePage(pageID)
	b.freeFrames = append(b.freeFrames, frameID)

	return nil
}

/*
UnpinPage unpins a page from the buffer pool for the current thread, potentially flagging the page as dirty.
If there are no more references to the page, the page is eligible for cache eviction.

If the page was not found this is a noop.
*/
func (b *BufferPool) UnpinPage(pageID PageID, isDirty bool) {
	if frameID, ok := b.pageLookup[pageID]; ok {
		page := b.pages[frameID]
		page.decrementPinCount()
		page.isDirty = page.isDirty || isDirty

		if page.pinCount == 0 {
			b.eviction.Add(frameID)
		}
	}
}

/*
UnpinAndFlushPage unpins the page and flushes it to disk.
If there are no more references to the page, the page is eligible for cache eviction.

If the page was not found this is a noop.

Returns an error only if flushing the page failed.
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

	}

	return nil
}

/*
getFrame returns a frame.
The frame may either be from the
- free frames list, or from
- cache eviction
If evicted, the frame gets removed from cache, potentially flushing to disk if the associated page was dirty.

The 2nd returned value indicates if the FrameID was evicted from eviction.
The 3rd returned value indicates an error if no frame could be allocated or flushing to disk failed.

Upon ANY error, the caller must make sure to re-add the FrameID into the eviction.
*/
func (b *BufferPool) getFrame() FrameHelper {
	var frameID *FrameID
	isEvicted := false

	// get next free frame or evict from cache
	if len(b.freeFrames) > 0 {
		frameID = &b.freeFrames[0]
		b.freeFrames = b.freeFrames[1:]
	} else {
		// no free frame found
		if frameID = b.eviction.Victim(); frameID == nil {
			return newFrameHelper(b, frameID, nil, errors.New("unable to reserve buffer frame"), false)
		}
		isEvicted = true
	}

	// if evicted from cache, update the table and write to disk when dirty
	if isEvicted {
		page := b.pages[*frameID]
		if page != nil {
			if page.isDirty {
				page.isDirty = false
				if err := b.disk.WritePage(page); err != nil {
					page.isDirty = true

					return newFrameHelper(b, frameID, nil, err, true)
				}
			}

			delete(b.pageLookup, page.id)
		}
	}

	return newFrameHelper(b, frameID, nil, nil, true)
}

// FrameHelper helps on
type FrameHelper struct {
	bufferPool *BufferPool
	frameID    *FrameID
	page       *Page
	err        error
	isEvicted  bool
}

func newFrameHelper(b *BufferPool, frameID *FrameID, page *Page, err error, isEvicted bool) FrameHelper {
	fh := FrameHelper{b, frameID, page, err, isEvicted}
	fh.rollBackOnErr()
	return fh
}

func (f *FrameHelper) rollBackOnErr() {
	if f.err != nil && f.isEvicted {
		f.bufferPool.eviction.Add(*f.frameID)
	}
}

func (f *FrameHelper) AllocatePage() {
	f.page, f.err = f.bufferPool.disk.AllocatePage()
	f.rollBackOnErr()
}

func (f *FrameHelper) ReadPage(id PageID) {
	f.page, f.err = f.bufferPool.disk.ReadPage(id)
	f.rollBackOnErr()
}
