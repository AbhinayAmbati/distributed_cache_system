package cluster

import (
	"testing"

	"github.com/AbhinayAmbati/distributed_cache_system/internal/hashing"
	"github.com/AbhinayAmbati/distributed_cache_system/internal/store"
)

func TestReplicationManager_IsPrimary(t *testing.T) {
	s := store.New()
	ring := hashing.NewRing(150, 0.25)
	registry := NewNodeRegistry("node-1")

	ring.AddNode("node-1")
	ring.AddNode("node-2")
	ring.AddNode("node-3")

	rm := NewReplicationManager("node-1", s, ring, registry, 3)

	// Check some keys — at least some should have node-1 as primary.
	primaryCount := 0
	for i := 0; i < 100; i++ {
		key := keyForTest(i)
		if rm.IsPrimary(key) {
			primaryCount++
		}
	}

	if primaryCount == 0 {
		t.Fatal("expected node-1 to be primary for some keys")
	}
	if primaryCount == 100 {
		t.Fatal("expected node-1 to NOT be primary for all keys")
	}

	t.Logf("node-1 is primary for %d / 100 keys", primaryCount)
}

func TestReplicationManager_IsReplica(t *testing.T) {
	s := store.New()
	ring := hashing.NewRing(150, 0.25)
	registry := NewNodeRegistry("node-1")

	ring.AddNode("node-1")
	ring.AddNode("node-2")
	ring.AddNode("node-3")

	rm := NewReplicationManager("node-1", s, ring, registry, 3)

	// With 3 nodes and replica count 3, every key should have node-1 as a replica.
	for i := 0; i < 100; i++ {
		key := keyForTest(i)
		if !rm.IsReplica(key) {
			t.Fatalf("expected node-1 to be a replica for key %s", key)
		}
	}
}

func TestReplicationManager_GetReplicaSet(t *testing.T) {
	s := store.New()
	ring := hashing.NewRing(150, 0.25)
	registry := NewNodeRegistry("node-1")

	ring.AddNode("node-1")
	ring.AddNode("node-2")
	ring.AddNode("node-3")

	rm := NewReplicationManager("node-1", s, ring, registry, 3)

	replicas := rm.GetReplicaSet("test-key")
	if len(replicas) != 3 {
		t.Fatalf("expected 3 replicas, got %d", len(replicas))
	}

	// All replicas should be unique.
	seen := make(map[string]bool)
	for _, r := range replicas {
		if seen[r] {
			t.Fatalf("duplicate replica: %s", r)
		}
		seen[r] = true
	}
}

func TestReplicationManager_GetReplicaSet_SmallCluster(t *testing.T) {
	s := store.New()
	ring := hashing.NewRing(150, 0.25)
	registry := NewNodeRegistry("node-1")

	ring.AddNode("node-1")
	ring.AddNode("node-2")

	// Request 3 replicas but only 2 nodes exist.
	rm := NewReplicationManager("node-1", s, ring, registry, 3)

	replicas := rm.GetReplicaSet("test-key")
	if len(replicas) != 2 {
		t.Fatalf("expected 2 replicas (limited by cluster size), got %d", len(replicas))
	}
}

func TestReplicationManager_GetPrimary(t *testing.T) {
	s := store.New()
	ring := hashing.NewRing(150, 0.25)
	registry := NewNodeRegistry("node-1")

	ring.AddNode("node-1")
	ring.AddNode("node-2")
	ring.AddNode("node-3")

	rm := NewReplicationManager("node-1", s, ring, registry, 3)

	primary, ok := rm.GetPrimary("test-key")
	if !ok {
		t.Fatal("expected to find a primary")
	}
	if primary == "" {
		t.Fatal("expected non-empty primary node ID")
	}
	t.Logf("primary for 'test-key': %s", primary)
}

func TestReplicationManager_PrimaryChangesOnNodeRemoval(t *testing.T) {
	s := store.New()
	ring := hashing.NewRing(150, 0.25)
	registry := NewNodeRegistry("node-1")

	ring.AddNode("node-1")
	ring.AddNode("node-2")
	ring.AddNode("node-3")

	rm := NewReplicationManager("node-1", s, ring, registry, 3)

	// Find a key where node-2 is the primary.
	var targetKey string
	for i := 0; i < 1000; i++ {
		key := keyForTest(i)
		primary, _ := rm.GetPrimary(key)
		if primary == "node-2" {
			targetKey = key
			break
		}
	}

	if targetKey == "" {
		t.Skip("could not find a key with node-2 as primary")
	}

	// Remove node-2 from the ring (simulating failure).
	ring.RemoveNode("node-2")

	// Primary should change.
	newPrimary, ok := rm.GetPrimary(targetKey)
	if !ok {
		t.Fatal("expected to find a new primary")
	}
	if newPrimary == "node-2" {
		t.Fatal("expected primary to change after node-2 removal")
	}

	t.Logf("key %s: primary changed from node-2 to %s", targetKey, newPrimary)
}

func TestReplicationManager_ReplicateWriteNoSecondaries(t *testing.T) {
	s := store.New()
	ring := hashing.NewRing(150, 0.25)
	registry := NewNodeRegistry("node-1")

	ring.AddNode("node-1")

	rm := NewReplicationManager("node-1", s, ring, registry, 1)

	// Should not panic with no secondaries.
	rm.ReplicateWrite("key", []byte("value"), 0)
}

func keyForTest(i int) string {
	return "test-key-" + string(rune('A'+i%26)) + "-" + string(rune('0'+i%10))
}
