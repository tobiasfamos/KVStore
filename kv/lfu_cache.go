package kv

import "time"

/*
LFUCache is a Least Frequently Used cache algorithm.
*/
type LFUCache struct {
	items map[FrameID]time.Time
}

func NewLFUCache(size uint) LFUCache {
	return LFUCache{
		items: make(map[FrameID]time.Time, size),
	}
}

func (c *LFUCache) Victim() *FrameID {
	if len(c.items) == 0 {
		return nil
	}

	oldestTime := time.Now()
	var oldestID FrameID

	for id, idTime := range c.items {
		if idTime.Before(oldestTime) {
			oldestTime = idTime
			oldestID = id
		}
	}

	delete(c.items, oldestID)

	return &oldestID
}

func (c *LFUCache) Remove(frameID FrameID) {
	delete(c.items, frameID)
}

func (c *LFUCache) Add(frameID FrameID) {
	c.items[frameID] = time.Now()
}
