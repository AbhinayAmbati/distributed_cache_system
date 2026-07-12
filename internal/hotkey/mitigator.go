package hotkey

import (
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/AbhinayAmbati/distributed_cache_system/internal/store"
)

// MitigationStrategy defines how to handle a hot key.
type MitigationStrategy int

const (
	// StrategyNone means no mitigation is applied.
	StrategyNone MitigationStrategy = iota
	// StrategyKeySplit splits the hot key across multiple sharded keys.
	StrategyKeySplit
	// StrategyReadReplica adds extra read replicas for the hot key.
	StrategyReadReplica
)

// String returns the strategy name.
func (s MitigationStrategy) String() string {
	switch s {
	case StrategyKeySplit:
		return "KEY_SPLIT"
	case StrategyReadReplica:
		return "READ_REPLICA"
	default:
		return "NONE"
	}
}

// Mitigator handles hot key mitigation strategies.
type Mitigator struct {
	mu sync.RWMutex

	store    *store.Store
	detector *Detector

	// Sharding config for key splitting.
	shardCount int // Number of shards for split keys.

	// Tracks which keys have active mitigations.
	mitigatedKeys map[string]MitigationStrategy

	// Stats.
	mitigationCount uint64
}

// NewMitigator creates a new hot key mitigator.
// shardCount is the number of shards to split hot keys into (e.g., 4).
func NewMitigator(s *store.Store, detector *Detector, shardCount int) *Mitigator {
	if shardCount < 2 {
		shardCount = 4
	}
	return &Mitigator{
		store:         s,
		detector:      detector,
		shardCount:    shardCount,
		mitigatedKeys: make(map[string]MitigationStrategy),
	}
}

// CheckAndMitigate checks if a key is hot and applies mitigation if needed.
// Returns true if the key is hot (caller can decide to hint the client).
func (m *Mitigator) CheckAndMitigate(key string) bool {
	isHot := m.detector.RecordAccess(key)
	if !isHot {
		return false
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if _, already := m.mitigatedKeys[key]; already {
		return true // Already mitigated.
	}

	// Apply key splitting for the hot key.
	m.applySplitting(key)
	m.mitigatedKeys[key] = StrategyKeySplit
	m.mitigationCount++

	log.Printf("[hotkey] mitigated hot key %q via KEY_SPLIT (%d shards)", key, m.shardCount)
	return true
}

// GetShardedKey returns the shard key for a hot key based on a client identifier.
// For non-hot keys, returns the original key.
func (m *Mitigator) GetShardedKey(key string, clientID string) string {
	m.mu.RLock()
	strategy, isMitigated := m.mitigatedKeys[key]
	m.mu.RUnlock()

	if !isMitigated || strategy != StrategyKeySplit {
		return key
	}

	// Hash the client ID to determine which shard to read from.
	shardIdx := hashString(clientID) % uint32(m.shardCount)
	return ShardKeyName(key, int(shardIdx))
}

// applySplitting creates sharded copies of a hot key.
func (m *Mitigator) applySplitting(key string) {
	// Read the original value.
	value, found := m.store.Get(key)
	if !found {
		return
	}

	// Create shard copies.
	for i := 0; i < m.shardCount; i++ {
		shardKey := ShardKeyName(key, i)
		m.store.Set(shardKey, value, 0) // Inherit original TTL behavior.
	}
}

// WriteToShards writes a value to all shards of a hot key.
// This should be called instead of a normal Set for hot keys.
func (m *Mitigator) WriteToShards(key string, value []byte, ttl time.Duration) {
	m.mu.RLock()
	strategy, isMitigated := m.mitigatedKeys[key]
	m.mu.RUnlock()

	// Always write to the original key.
	m.store.Set(key, value, ttl)

	if !isMitigated || strategy != StrategyKeySplit {
		return
	}

	// Also write to all shard copies.
	for i := 0; i < m.shardCount; i++ {
		shardKey := ShardKeyName(key, i)
		m.store.Set(shardKey, value, ttl)
	}
}

// IsMitigated returns true if a key has active mitigation.
func (m *Mitigator) IsMitigated(key string) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	_, ok := m.mitigatedKeys[key]
	return ok
}

// GetMitigatedKeys returns a snapshot of all mitigated keys and their strategies.
func (m *Mitigator) GetMitigatedKeys() map[string]MitigationStrategy {
	m.mu.RLock()
	defer m.mu.RUnlock()

	result := make(map[string]MitigationStrategy, len(m.mitigatedKeys))
	for k, v := range m.mitigatedKeys {
		result[k] = v
	}
	return result
}

// MitigationCount returns the total number of mitigations applied.
func (m *Mitigator) MitigationCount() uint64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.mitigationCount
}

// ShardKeyName returns the shard key name for a given key and shard index.
func ShardKeyName(key string, shardIndex int) string {
	return fmt.Sprintf("%s:shard_%d", key, shardIndex)
}

// hashString returns a simple hash of a string (for shard selection).
func hashString(s string) uint32 {
	var h uint32 = 2166136261 // FNV offset basis.
	for i := 0; i < len(s); i++ {
		h ^= uint32(s[i])
		h *= 16777619 // FNV prime.
	}
	return h
}
