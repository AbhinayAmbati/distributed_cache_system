package store

import (
	"log"
	"math/rand"
	"sync"
	"time"
)

const (
	// DefaultSweepInterval is how often the background sweeper checks a shard.
	DefaultSweepInterval = 100 * time.Millisecond

	// samplesPerSweep is the number of random keys sampled per shard per sweep cycle.
	samplesPerSweep = 20

	// expiredThreshold — if more than 25% of sampled keys are expired, sweep again immediately.
	expiredThreshold = 0.25
)

// Sweeper runs a background goroutine that actively expires stale entries.
// It uses a Redis-inspired approach: sample random keys from each shard
// and delete expired ones. If the expiry rate is high, repeat immediately.
type Sweeper struct {
	store    *Store
	interval time.Duration
	stopCh   chan struct{}
	wg       sync.WaitGroup
}

// NewSweeper creates a new TTL sweeper for the given store.
func NewSweeper(store *Store, interval time.Duration) *Sweeper {
	if interval <= 0 {
		interval = DefaultSweepInterval
	}
	return &Sweeper{
		store:    store,
		interval: interval,
		stopCh:   make(chan struct{}),
	}
}

// Start begins the background sweep goroutine.
func (sw *Sweeper) Start() {
	sw.wg.Add(1)
	go sw.run()
	log.Printf("[sweeper] started with interval %v", sw.interval)
}

// Stop signals the sweeper to stop and waits for it to finish.
func (sw *Sweeper) Stop() {
	close(sw.stopCh)
	sw.wg.Wait()
	log.Printf("[sweeper] stopped")
}

// run is the main sweep loop. It cycles through shards one at a time.
func (sw *Sweeper) run() {
	defer sw.wg.Done()

	shardIndex := 0
	ticker := time.NewTicker(sw.interval)
	defer ticker.Stop()

	for {
		select {
		case <-sw.stopCh:
			return
		case <-ticker.C:
			sw.sweepShard(shardIndex)
			shardIndex = (shardIndex + 1) % len(sw.store.shards)
		}
	}
}

// sweepShard samples random keys from a shard and deletes expired ones.
// If the expiry rate exceeds the threshold, it sweeps again immediately.
func (sw *Sweeper) sweepShard(index int) {
	sh := sw.store.shards[index]

	for {
		expired := sw.sampleAndExpire(sh)
		if expired < expiredThreshold {
			break
		}
		// High expiry rate — sweep this shard again immediately.
		select {
		case <-sw.stopCh:
			return
		default:
		}
	}
}

// sampleAndExpire samples up to samplesPerSweep keys from a shard,
// deletes expired entries, and returns the fraction that were expired.
func (sw *Sweeper) sampleAndExpire(sh *shard) float64 {
	sh.mu.Lock()
	defer sh.mu.Unlock()

	n := len(sh.items)
	if n == 0 {
		return 0
	}

	// Collect all keys to enable random sampling.
	keys := make([]string, 0, n)
	for k := range sh.items {
		keys = append(keys, k)
	}

	// Sample up to samplesPerSweep keys.
	samples := samplesPerSweep
	if samples > n {
		samples = n
	}

	expiredCount := 0
	now := time.Now()

	for i := 0; i < samples; i++ {
		// Pick a random key using Fisher-Yates-style selection.
		idx := rand.Intn(len(keys)-i) + i
		keys[i], keys[idx] = keys[idx], keys[i]
		key := keys[i]

		entry := sh.items[key]
		if !entry.ExpiresAt.IsZero() && now.After(entry.ExpiresAt) {
			delete(sh.items, key)
			expiredCount++
			sw.store.stats.incExpirations()
		}
	}

	if samples == 0 {
		return 0
	}
	return float64(expiredCount) / float64(samples)
}
