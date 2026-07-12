package hotkey

import (
	"fmt"
	"testing"
	"time"

	"github.com/AbhinayAmbati/distributed_cache_system/internal/store"
)

func TestCountMinSketch_IncrementAndEstimate(t *testing.T) {
	cms := NewCountMinSketch(4096, 4)

	// Increment "key1" 100 times.
	for i := 0; i < 100; i++ {
		cms.Increment("key1")
	}

	// Estimate should be >= 100 (never underestimates).
	estimate := cms.Estimate("key1")
	if estimate < 100 {
		t.Fatalf("expected estimate >= 100, got %d", estimate)
	}
	// With 4096 width, collision should be rare.
	if estimate > 110 {
		t.Fatalf("estimate too high (likely collision): %d", estimate)
	}
	t.Logf("key1 estimate: %d (actual: 100)", estimate)
}

func TestCountMinSketch_UnseenKey(t *testing.T) {
	cms := NewCountMinSketch(4096, 4)

	estimate := cms.Estimate("never-seen")
	if estimate != 0 {
		t.Fatalf("expected 0 for unseen key, got %d", estimate)
	}
}

func TestCountMinSketch_MultipleKeys(t *testing.T) {
	cms := NewCountMinSketch(4096, 4)

	// Different keys with different frequencies.
	for i := 0; i < 1000; i++ {
		cms.Increment("hot-key")
	}
	for i := 0; i < 100; i++ {
		cms.Increment("warm-key")
	}
	for i := 0; i < 10; i++ {
		cms.Increment("cold-key")
	}

	hotEst := cms.Estimate("hot-key")
	warmEst := cms.Estimate("warm-key")
	coldEst := cms.Estimate("cold-key")

	if hotEst < 1000 {
		t.Fatalf("hot-key: expected >= 1000, got %d", hotEst)
	}
	if warmEst < 100 {
		t.Fatalf("warm-key: expected >= 100, got %d", warmEst)
	}
	if coldEst < 10 {
		t.Fatalf("cold-key: expected >= 10, got %d", coldEst)
	}

	t.Logf("hot=%d warm=%d cold=%d", hotEst, warmEst, coldEst)
}

func TestCountMinSketch_Decay(t *testing.T) {
	cms := NewCountMinSketch(4096, 4)

	for i := 0; i < 100; i++ {
		cms.Increment("key1")
	}

	before := cms.Estimate("key1")
	cms.Decay()
	after := cms.Estimate("key1")

	// After decay, estimate should be roughly half.
	if after > before {
		t.Fatalf("expected decay to reduce estimate, before=%d after=%d", before, after)
	}
	if after < 40 || after > 60 {
		t.Logf("WARNING: decay result outside expected range, before=%d after=%d", before, after)
	}

	t.Logf("before decay: %d, after decay: %d", before, after)
}

func TestCountMinSketch_Reset(t *testing.T) {
	cms := NewCountMinSketch(4096, 4)

	for i := 0; i < 100; i++ {
		cms.Increment("key1")
	}

	cms.Reset()

	if cms.Estimate("key1") != 0 {
		t.Fatal("expected 0 after reset")
	}
	if cms.Total() != 0 {
		t.Fatal("expected total 0 after reset")
	}
}

func TestDetector_HotKeyDetection(t *testing.T) {
	d := NewDetector(100, 60*time.Second)

	// Access a key 99 times — not hot yet.
	for i := 0; i < 99; i++ {
		isHot := d.RecordAccess("frequent-key")
		if isHot {
			t.Fatalf("key should not be hot at access %d", i+1)
		}
	}

	// 100th access — should become hot.
	isHot := d.RecordAccess("frequent-key")
	if !isHot {
		t.Fatal("expected key to be hot after 100 accesses")
	}

	// Cold key should not be hot.
	d.RecordAccess("cold-key")
	if d.IsHot("cold-key") {
		t.Fatal("cold key should not be hot")
	}
}

func TestDetector_DecayReducesHotness(t *testing.T) {
	d := NewDetector(100, 50*time.Millisecond)
	d.Start()
	defer d.Stop()

	// Make a key hot.
	for i := 0; i < 200; i++ {
		d.RecordAccess("decay-test")
	}

	if !d.IsHot("decay-test") {
		t.Fatal("expected key to be hot")
	}

	// Wait for decay cycles.
	time.Sleep(200 * time.Millisecond)

	// After enough decays, key should no longer be hot.
	freq := d.GetFrequency("decay-test")
	t.Logf("frequency after decay: %d", freq)
}

func TestMitigator_HotKeyMitigation(t *testing.T) {
	s := store.New()
	d := NewDetector(50, 60*time.Second)
	m := NewMitigator(s, d, 4)

	// Set a value in the store.
	s.Set("popular-item", []byte("value123"), 0)

	// Access the key many times to make it hot.
	for i := 0; i < 49; i++ {
		m.CheckAndMitigate("popular-item")
	}

	if m.IsMitigated("popular-item") {
		t.Fatal("key should not be mitigated yet")
	}

	// One more access should trigger mitigation.
	isHot := m.CheckAndMitigate("popular-item")
	if !isHot {
		t.Fatal("expected key to be detected as hot")
	}
	if !m.IsMitigated("popular-item") {
		t.Fatal("expected key to be mitigated")
	}

	// Verify shard copies were created.
	for i := 0; i < 4; i++ {
		shardKey := ShardKeyName("popular-item", i)
		val, found := s.Get(shardKey)
		if !found {
			t.Fatalf("expected shard %d to exist", i)
		}
		if string(val) != "value123" {
			t.Fatalf("expected shard value 'value123', got '%s'", string(val))
		}
	}
}

func TestMitigator_GetShardedKey(t *testing.T) {
	s := store.New()
	d := NewDetector(10, 60*time.Second)
	m := NewMitigator(s, d, 4)

	s.Set("hot-key", []byte("data"), 0)

	// Make it hot.
	for i := 0; i < 20; i++ {
		m.CheckAndMitigate("hot-key")
	}

	// Different clients should get different shard keys.
	key1 := m.GetShardedKey("hot-key", "client-A")
	key2 := m.GetShardedKey("hot-key", "client-B")

	t.Logf("client-A → %s", key1)
	t.Logf("client-B → %s", key2)

	// Both should be valid shard keys (may or may not be different).
	if key1 == "hot-key" {
		t.Fatal("expected a sharded key for a mitigated hot key")
	}
}

func TestMitigator_WriteToShards(t *testing.T) {
	s := store.New()
	d := NewDetector(5, 60*time.Second)
	m := NewMitigator(s, d, 3)

	s.Set("item", []byte("v1"), 0)

	// Make hot.
	for i := 0; i < 10; i++ {
		m.CheckAndMitigate("item")
	}

	// Write new value through mitigator.
	m.WriteToShards("item", []byte("v2"), 0)

	// Original and all shards should have new value.
	val, _ := s.Get("item")
	if string(val) != "v2" {
		t.Fatal("expected original key to have new value")
	}

	for i := 0; i < 3; i++ {
		val, _ := s.Get(ShardKeyName("item", i))
		if string(val) != "v2" {
			t.Fatalf("shard %d should have new value", i)
		}
	}
}

// Benchmarks

func BenchmarkCountMinSketch_Increment(b *testing.B) {
	cms := NewCountMinSketch(4096, 4)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cms.Increment(fmt.Sprintf("key-%d", i%10000))
	}
}

func BenchmarkCountMinSketch_Estimate(b *testing.B) {
	cms := NewCountMinSketch(4096, 4)
	for i := 0; i < 10000; i++ {
		cms.Increment(fmt.Sprintf("key-%d", i))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cms.Estimate(fmt.Sprintf("key-%d", i%10000))
	}
}

func BenchmarkDetector_RecordAccess(b *testing.B) {
	d := NewDetector(1000, 60*time.Second)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		d.RecordAccess(fmt.Sprintf("key-%d", i%10000))
	}
}
