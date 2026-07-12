package hotkey

import (
	"hash/fnv"
	"math"
	"sync"
	"time"
)

// CountMinSketch is a probabilistic data structure for frequency estimation.
// It uses d independent hash functions and a 2D array of counters to
// estimate the frequency of elements in a stream with bounded error.
type CountMinSketch struct {
	mu      sync.RWMutex
	width   uint32     // w: number of counters per row.
	depth   uint32     // d: number of hash functions (rows).
	table   [][]uint64 // d × w array of counters.
	seeds   []uint64   // Random seeds for hash functions.
	total   uint64     // Total increments across all keys.
}

// NewCountMinSketch creates a new Count-Min Sketch.
// width (w) controls accuracy — larger = more accurate.
// depth (d) controls confidence — more rows = lower false positive rate.
// Recommended: d=4, w=4096 (16KB memory).
func NewCountMinSketch(width, depth uint32) *CountMinSketch {
	if width == 0 {
		width = 4096
	}
	if depth == 0 {
		depth = 4
	}

	table := make([][]uint64, depth)
	seeds := make([]uint64, depth)
	for i := uint32(0); i < depth; i++ {
		table[i] = make([]uint64, width)
		// Use different seeds for each hash function.
		seeds[i] = uint64(i*2654435761 + 1) // Knuth's multiplicative hash.
	}

	return &CountMinSketch{
		width: width,
		depth: depth,
		table: table,
		seeds: seeds,
	}
}

// Increment adds 1 to the frequency estimate for the given key.
func (cms *CountMinSketch) Increment(key string) {
	cms.mu.Lock()
	defer cms.mu.Unlock()

	for i := uint32(0); i < cms.depth; i++ {
		idx := cms.hash(key, i) % cms.width
		cms.table[i][idx]++
	}
	cms.total++
}

// Estimate returns the estimated frequency of the given key.
// The estimate is always >= actual count (never underestimates).
func (cms *CountMinSketch) Estimate(key string) uint64 {
	cms.mu.RLock()
	defer cms.mu.RUnlock()

	var min uint64 = math.MaxUint64
	for i := uint32(0); i < cms.depth; i++ {
		idx := cms.hash(key, i) % cms.width
		if cms.table[i][idx] < min {
			min = cms.table[i][idx]
		}
	}

	if min == math.MaxUint64 {
		return 0
	}
	return min
}

// Decay halves all counters. This is used periodically to handle
// changing access patterns — old hot keys gradually cool down.
func (cms *CountMinSketch) Decay() {
	cms.mu.Lock()
	defer cms.mu.Unlock()

	for i := uint32(0); i < cms.depth; i++ {
		for j := uint32(0); j < cms.width; j++ {
			cms.table[i][j] /= 2
		}
	}
	cms.total /= 2
}

// Reset zeroes all counters.
func (cms *CountMinSketch) Reset() {
	cms.mu.Lock()
	defer cms.mu.Unlock()

	for i := uint32(0); i < cms.depth; i++ {
		for j := uint32(0); j < cms.width; j++ {
			cms.table[i][j] = 0
		}
	}
	cms.total = 0
}

// Total returns the total number of increments.
func (cms *CountMinSketch) Total() uint64 {
	cms.mu.RLock()
	defer cms.mu.RUnlock()
	return cms.total
}

// hash computes the hash for a key using the i-th hash function.
func (cms *CountMinSketch) hash(key string, i uint32) uint32 {
	h := fnv.New32a()
	// Mix in the seed to create independent hash functions.
	seed := cms.seeds[i]
	seedBytes := []byte{
		byte(seed), byte(seed >> 8), byte(seed >> 16), byte(seed >> 24),
	}
	h.Write(seedBytes)
	h.Write([]byte(key))
	return h.Sum32()
}

// --- Hot Key Detector ---

// Detector uses a Count-Min Sketch to detect hot keys in real-time.
type Detector struct {
	sketch       *CountMinSketch
	hotThreshold uint64        // Minimum count to be considered hot.
	decayPeriod  time.Duration // How often to decay the sketch.
	stopCh       chan struct{}
	wg           sync.WaitGroup
}

// NewDetector creates a new hot key detector.
// hotThreshold: minimum estimated frequency to consider a key "hot".
// decayPeriod: how often to halve all counters (e.g., 60 seconds).
func NewDetector(hotThreshold uint64, decayPeriod time.Duration) *Detector {
	return &Detector{
		sketch:       NewCountMinSketch(4096, 4),
		hotThreshold: hotThreshold,
		decayPeriod:  decayPeriod,
		stopCh:       make(chan struct{}),
	}
}

// RecordAccess records an access to a key and returns whether the key is hot.
func (d *Detector) RecordAccess(key string) bool {
	d.sketch.Increment(key)
	return d.sketch.Estimate(key) >= d.hotThreshold
}

// IsHot checks if a key is currently hot without recording an access.
func (d *Detector) IsHot(key string) bool {
	return d.sketch.Estimate(key) >= d.hotThreshold
}

// GetFrequency returns the estimated access frequency for a key.
func (d *Detector) GetFrequency(key string) uint64 {
	return d.sketch.Estimate(key)
}

// Start begins the background decay goroutine.
func (d *Detector) Start() {
	d.wg.Add(1)
	go d.decayLoop()
}

// Stop stops the background decay goroutine.
func (d *Detector) Stop() {
	close(d.stopCh)
	d.wg.Wait()
}

// decayLoop periodically decays the sketch to handle changing patterns.
func (d *Detector) decayLoop() {
	defer d.wg.Done()

	ticker := time.NewTicker(d.decayPeriod)
	defer ticker.Stop()

	for {
		select {
		case <-d.stopCh:
			return
		case <-ticker.C:
			d.sketch.Decay()
		}
	}
}
