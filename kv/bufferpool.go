package kv

import (
	"errors"
	"log"
)

const MaxPoolSize = 16

type FrameID uint32

/*
BufferPool is a cache-like structure that buffers Pages from a Disk.
*/
type BufferPool struct {
	disk       Disk
	pages      [MaxPoolSize]*Page
	pageLookup map[PageID]FrameID
	eviction   CacheEviction
	freeFrames []FrameID
}

/*
NewPage allocates a new page on the disk and caches it to buffer pool.

This method returns nil if there are
- no free frames,
- no frame can be evicted from buffer, or
the disk is full.
*/
func (b *BufferPool) NewPage() *Page {
	// get next free frame or evict from cache
	frameID := b.getFrame()
	if frameID == nil {
		return nil
	}

	// allocate new page from disk
	pageID := b.disk.AllocatePage()
	if pageID == nil {
		return nil
	}

	page := &Page{
		id:       *pageID,
		pinCount: 1,
		isDirty:  false,
		isLeaf:   false,
		data:     [PageDataSize]byte{},
	}
	b.pageLookup[*pageID] = *frameID
	b.pages[*frameID] = page

	return page
}

/*
FetchPage fetches a page.

This method returns nil if there are
- no free frames in buffer,
- no frame can be evicted from buffer, or
- the page cannot be found on disk
*/
func (b *BufferPool) FetchPage(pageID PageID) *Page {
	// try fetch from cache
	if frameID, ok := b.pageLookup[pageID]; ok {
		page := b.pages[frameID]
		page.pinCount++
		b.eviction.Remove(frameID)

		return page
	}

	// get next free frame or evict from cache
	frameID := b.getFrame()
	if frameID == nil {
		return nil
	}

	// try fetch from disk
	page, err := b.disk.ReadPage(pageID)
	if err != nil {
		log.Println(err.Error())
		return nil
	}

	page.pinCount = 1
	b.pageLookup[pageID] = *frameID
	b.pages[*frameID] = page

	return page
}

/*
FlushPage flushes a page to disk.

Returns an error only if the page was not found.
*/
func (b *BufferPool) FlushPage(pageID PageID) error {
	if frameID, ok := b.pageLookup[pageID]; ok {
		page := b.pages[frameID]
		page.decrementPinCount()

		if err := b.disk.WritePage(page); err != nil {
			log.Println(err.Error())
		}
		page.isDirty = false

		return nil
	}

	return errors.New("page not found")
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
		return errors.New("inconsistent state: page.id != pageID") // good to catch logic bugs
	}

	delete(b.pageLookup, pageID)
	b.eviction.Remove(frameID)
	b.disk.DeallocatePage(pageID)
	b.freeFrames = append(b.freeFrames, frameID)

	return nil
}

/*
UnpinPage unpins a page from the buffer pool for the current thread, potentially flagging the page as dirty.
If no more threads are using the page, the page is eligible for cache eviction.

Returns an error only if the page was not found.
*/
func (b *BufferPool) UnpinPage(pageID PageID, isDirty bool) error {
	if frameID, ok := b.pageLookup[pageID]; ok {
		page := b.pages[frameID]
		page.decrementPinCount()

		if page.pinCount == 0 {
			b.eviction.Add(frameID)
		}

		page.isDirty = page.isDirty || isDirty

		return nil
	}

	return errors.New("page not found")
}

func NewBufferPool() BufferPool {
	dist := NewRAMDisk(120000)
	pageLookup := make(map[PageID]FrameID)
	lfuCache := NewLFUCache(1200000)
	var freeFrames []FrameID
	localBufferPool := BufferPool{
		disk:       dist,
		pages:      [16]*Page{},
		pageLookup: pageLookup,
		eviction:   &lfuCache,
		freeFrames: freeFrames,
	}
	return localBufferPool
}

/*
getFrame returns a frame.
The frame may either be from the
- free frames list, or from
- cache eviction
If evicted, the frame gets removed from cache, potentially updating the disk if the associated page was dirty.

This method returns nil if there are no free frames and no frame can be evicted from cache.
*/
func (b *BufferPool) getFrame() *FrameID {
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
		return nil
	}

	// if evicted from cache, update the table and write to disk when changed
	if isEvicted {
		currPage := b.pages[*frameID]
		if currPage != nil {
			if currPage.isDirty {
				if err := b.disk.WritePage(currPage); err != nil {
					log.Println(err.Error())
				}
			}

			delete(b.pageLookup, currPage.id)
		}
	}

	return frameID
}
