package kv

type CacheEviction interface {
	// Victim elects a victim to evict.
	Victim() *FrameID
	// Remove a frame from eviction election.
	Remove(FrameID)
	// Add a frame for eviction election.
	Add(FrameID)
}
