package store

import (
	"hash/fnv"
	"sync"
	"time"
)

const (
	// DefaultShardCount is the number of shards used to reduce lock contention.
	DefaultShardCount = 256
)

// Entry represents a single cached value with metadata.
type Entry struct {
	Value     []byte
	ExpiresAt time.Time // Zero value means no expiry.
	CreatedAt time.Time
}

// IsExpired returns true if the entry has a TTL and it has passed.
func (e *Entry) IsExpired() bool {
	if e.ExpiresAt.IsZero() {
		return false
	}
	return time.Now().After(e.ExpiresAt)
}

// shard is a single partition of the key space, with its own lock.
type shard struct {
	mu    sync.RWMutex
	items map[string]*Entry
}

// Store is a sharded in-memory key-value store with TTL support.
// It distributes keys across multiple shards to minimize lock contention.
type Store struct {
	shards    []*shard
	shardMask uint32

	// Metrics (atomic counters)
	stats StoreStats
}

// StoreStats tracks operational metrics for the store.
type StoreStats struct {
	mu          sync.RWMutex
	Hits        uint64
	Misses      uint64
	Sets        uint64
	Deletes     uint64
	Expirations uint64
}

// Snapshot returns a copy of the current stats.
func (s *StoreStats) Snapshot() StoreStats {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return StoreStats{
		Hits:        s.Hits,
		Misses:      s.Misses,
		Sets:        s.Sets,
		Deletes:     s.Deletes,
		Expirations: s.Expirations,
	}
}

func (s *StoreStats) incHits() {
	s.mu.Lock()
	s.Hits++
	s.mu.Unlock()
}

func (s *StoreStats) incMisses() {
	s.mu.Lock()
	s.Misses++
	s.mu.Unlock()
}

func (s *StoreStats) incSets() {
	s.mu.Lock()
	s.Sets++
	s.mu.Unlock()
}

func (s *StoreStats) incDeletes() {
	s.mu.Lock()
	s.Deletes++
	s.mu.Unlock()
}

func (s *StoreStats) incExpirations() {
	s.mu.Lock()
	s.Expirations++
	s.mu.Unlock()
}

// New creates a new Store with the default number of shards.
func New() *Store {
	return NewWithShards(DefaultShardCount)
}

// NewWithShards creates a new Store with the specified number of shards.
// shardCount should be a power of 2 for efficient masking.
func NewWithShards(shardCount int) *Store {
	// Ensure shard count is a power of 2.
	if shardCount <= 0 {
		shardCount = DefaultShardCount
	}
	// Round up to the nearest power of 2.
	n := 1
	for n < shardCount {
		n <<= 1
	}
	shardCount = n

	shards := make([]*shard, shardCount)
	for i := range shards {
		shards[i] = &shard{
			items: make(map[string]*Entry),
		}
	}

	return &Store{
		shards:    shards,
		shardMask: uint32(shardCount - 1),
	}
}

// getShard returns the shard responsible for the given key.
func (s *Store) getShard(key string) *shard {
	h := fnv.New32a()
	h.Write([]byte(key))
	return s.shards[h.Sum32()&s.shardMask]
}

// Get retrieves the value for a key. Returns the value, whether it exists,
// and any error. Performs lazy expiry — expired entries are deleted on access.
func (s *Store) Get(key string) ([]byte, bool) {
	sh := s.getShard(key)

	sh.mu.RLock()
	entry, exists := sh.items[key]
	if !exists {
		sh.mu.RUnlock()
		s.stats.incMisses()
		return nil, false
	}

	// Check for lazy expiry.
	if entry.IsExpired() {
		sh.mu.RUnlock()
		// Upgrade to write lock to delete the expired entry.
		sh.mu.Lock()
		// Double-check after acquiring write lock.
		if e, ok := sh.items[key]; ok && e.IsExpired() {
			delete(sh.items, key)
			s.stats.incExpirations()
		}
		sh.mu.Unlock()
		s.stats.incMisses()
		return nil, false
	}

	// Make a copy of the value to avoid data races.
	val := make([]byte, len(entry.Value))
	copy(val, entry.Value)
	sh.mu.RUnlock()

	s.stats.incHits()
	return val, true
}

// Set stores a key-value pair with an optional TTL. If ttl is 0, the entry
// never expires. Overwrites any existing entry for the key.
func (s *Store) Set(key string, value []byte, ttl time.Duration) {
	sh := s.getShard(key)

	entry := &Entry{
		Value:     make([]byte, len(value)),
		CreatedAt: time.Now(),
	}
	copy(entry.Value, value)

	if ttl > 0 {
		entry.ExpiresAt = time.Now().Add(ttl)
	}

	sh.mu.Lock()
	sh.items[key] = entry
	sh.mu.Unlock()

	s.stats.incSets()
}

// Delete removes a key from the store. Returns true if the key existed.
func (s *Store) Delete(key string) bool {
	sh := s.getShard(key)

	sh.mu.Lock()
	_, existed := sh.items[key]
	if existed {
		delete(sh.items, key)
	}
	sh.mu.Unlock()

	if existed {
		s.stats.incDeletes()
	}
	return existed
}

// Expire updates the TTL on an existing key. Returns true if the key was found.
// A ttl of 0 removes the expiry (makes the key persistent).
func (s *Store) Expire(key string, ttl time.Duration) bool {
	sh := s.getShard(key)

	sh.mu.Lock()
	defer sh.mu.Unlock()

	entry, exists := sh.items[key]
	if !exists || entry.IsExpired() {
		return false
	}

	if ttl > 0 {
		entry.ExpiresAt = time.Now().Add(ttl)
	} else {
		entry.ExpiresAt = time.Time{} // Remove expiry.
	}
	return true
}

// Keys returns all non-expired keys in the store.
// This is intended for debugging — it scans all shards.
func (s *Store) Keys() []string {
	var keys []string
	now := time.Now()

	for _, sh := range s.shards {
		sh.mu.RLock()
		for k, v := range sh.items {
			if v.ExpiresAt.IsZero() || now.Before(v.ExpiresAt) {
				keys = append(keys, k)
			}
		}
		sh.mu.RUnlock()
	}

	return keys
}

// Len returns the count of non-expired entries.
func (s *Store) Len() int {
	count := 0
	now := time.Now()

	for _, sh := range s.shards {
		sh.mu.RLock()
		for _, v := range sh.items {
			if v.ExpiresAt.IsZero() || now.Before(v.ExpiresAt) {
				count++
			}
		}
		sh.mu.RUnlock()
	}

	return count
}

// ForEach iterates over all non-expired entries, calling fn for each one.
// The callback receives a copy of the value. If fn returns false, iteration stops.
func (s *Store) ForEach(fn func(key string, value []byte, ttl time.Duration) bool) {
	now := time.Now()

	for _, sh := range s.shards {
		sh.mu.RLock()
		for k, v := range sh.items {
			if !v.ExpiresAt.IsZero() && now.After(v.ExpiresAt) {
				continue // Skip expired.
			}

			var remainingTTL time.Duration
			if !v.ExpiresAt.IsZero() {
				remainingTTL = v.ExpiresAt.Sub(now)
			}

			valCopy := make([]byte, len(v.Value))
			copy(valCopy, v.Value)

			if !fn(k, valCopy, remainingTTL) {
				sh.mu.RUnlock()
				return
			}
		}
		sh.mu.RUnlock()
	}
}

// Stats returns a snapshot of the store's operational metrics.
func (s *Store) Stats() StoreStats {
	return s.stats.Snapshot()
}

// ShardStats returns per-shard item counts (for debugging/metrics).
func (s *Store) ShardStats() []int {
	counts := make([]int, len(s.shards))
	for i, sh := range s.shards {
		sh.mu.RLock()
		counts[i] = len(sh.items)
		sh.mu.RUnlock()
	}
	return counts
}
