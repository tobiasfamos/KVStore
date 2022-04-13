package kv

import "time"

/*
LRUCache is a Least Recently Used cache algorithm.
*/
type LRUCache struct {
	items map[FrameID]time.Time
}

func NewLRUCache(size uint) LRUCache {
	return LRUCache{
		items: make(map[FrameID]time.Time, size),
	}
}

func (c *LRUCache) Victim() *FrameID {
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

func (c *LRUCache) Remove(frameID FrameID) {
	delete(c.items, frameID)
}

func (c *LRUCache) Add(frameID FrameID) {
	c.items[frameID] = time.Now()
}
