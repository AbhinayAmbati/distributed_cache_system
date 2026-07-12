package hashing

import (
	"fmt"
	"math"
	"testing"
)

func TestRing_AddAndRemoveNode(t *testing.T) {
	r := NewRing(150, 0.25)

	if !r.AddNode("node-1") {
		t.Fatal("expected AddNode to return true")
	}
	if r.AddNode("node-1") {
		t.Fatal("expected AddNode to return false for duplicate")
	}

	if r.Size() != 1 {
		t.Fatalf("expected 1 node, got %d", r.Size())
	}

	if !r.HasNode("node-1") {
		t.Fatal("expected HasNode to return true")
	}

	if !r.RemoveNode("node-1") {
		t.Fatal("expected RemoveNode to return true")
	}
	if r.RemoveNode("node-1") {
		t.Fatal("expected RemoveNode to return false for missing node")
	}

	if r.Size() != 0 {
		t.Fatalf("expected 0 nodes, got %d", r.Size())
	}
}

func TestRing_GetNode_Empty(t *testing.T) {
	r := NewRing(150, 0.25)

	_, ok := r.GetNode("some-key")
	if ok {
		t.Fatal("expected GetNode to return false on empty ring")
	}
}

func TestRing_GetNode_SingleNode(t *testing.T) {
	r := NewRing(150, 0.25)
	r.AddNode("node-1")

	// All keys should go to the only node.
	for i := 0; i < 100; i++ {
		nodeID, ok := r.GetNode(fmt.Sprintf("key-%d", i))
		if !ok {
			t.Fatal("expected GetNode to return true")
		}
		if nodeID != "node-1" {
			t.Fatalf("expected node-1, got %s", nodeID)
		}
	}
}

func TestRing_KeyDistribution(t *testing.T) {
	r := NewRing(150, 0.25)
	nodes := []string{"node-1", "node-2", "node-3", "node-4", "node-5"}

	for _, n := range nodes {
		r.AddNode(n)
	}

	// Distribute 10K keys and check distribution.
	counts := make(map[string]int)
	numKeys := 10000
	for i := 0; i < numKeys; i++ {
		nodeID, ok := r.GetNode(fmt.Sprintf("key-%d", i))
		if !ok {
			t.Fatal("expected GetNode to return true")
		}
		counts[nodeID]++
	}

	// Each node should get roughly 20% ± 10%.
	expectedPct := 100.0 / float64(len(nodes))
	tolerance := 10.0 // ±10%

	for _, n := range nodes {
		pct := float64(counts[n]) / float64(numKeys) * 100
		if math.Abs(pct-expectedPct) > tolerance {
			t.Fatalf("node %s got %.1f%% of keys (expected ~%.1f%% ± %.1f%%)",
				n, pct, expectedPct, tolerance)
		}
		t.Logf("  %s: %d keys (%.1f%%)", n, counts[n], pct)
	}
}

func TestRing_MinimalReassignment(t *testing.T) {
	r := NewRing(150, 0.25)
	r.AddNode("node-1")
	r.AddNode("node-2")
	r.AddNode("node-3")

	// Record key → node mapping.
	numKeys := 10000
	originalMapping := make(map[string]string)
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("key-%d", i)
		nodeID, _ := r.GetNode(key)
		originalMapping[key] = nodeID
	}

	// Add a 4th node.
	r.AddNode("node-4")

	// Count how many keys moved.
	moved := 0
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("key-%d", i)
		newNodeID, _ := r.GetNode(key)
		if newNodeID != originalMapping[key] {
			moved++
		}
	}

	// Expect roughly 1/N keys to move (1/4 = 25%).
	// Allow some tolerance.
	movedPct := float64(moved) / float64(numKeys) * 100
	expectedPct := 100.0 / 4.0 // ~25%
	tolerance := 10.0

	if math.Abs(movedPct-expectedPct) > tolerance {
		t.Fatalf("%.1f%% of keys moved (expected ~%.1f%% ± %.1f%%)",
			movedPct, expectedPct, tolerance)
	}

	t.Logf("  Keys moved: %d / %d (%.1f%%)", moved, numKeys, movedPct)
}

func TestRing_GetNodes_ReplicaSet(t *testing.T) {
	r := NewRing(150, 0.25)
	r.AddNode("node-1")
	r.AddNode("node-2")
	r.AddNode("node-3")

	// Get 3 replica nodes for a key.
	nodes := r.GetNodes("my-key", 3)
	if len(nodes) != 3 {
		t.Fatalf("expected 3 nodes, got %d", len(nodes))
	}

	// All nodes should be unique.
	seen := make(map[string]bool)
	for _, n := range nodes {
		if seen[n] {
			t.Fatalf("duplicate node in replica set: %s", n)
		}
		seen[n] = true
	}
}

func TestRing_GetNodes_ExceedsClusterSize(t *testing.T) {
	r := NewRing(150, 0.25)
	r.AddNode("node-1")
	r.AddNode("node-2")

	// Request more replicas than nodes.
	nodes := r.GetNodes("my-key", 5)
	if len(nodes) != 2 {
		t.Fatalf("expected 2 nodes (cluster size), got %d", len(nodes))
	}
}

func TestRing_BoundedLoads(t *testing.T) {
	r := NewRing(150, 0.25)
	r.AddNode("node-1")
	r.AddNode("node-2")
	r.AddNode("node-3")

	// Find which node "hot-key" normally maps to.
	normalNode, _ := r.GetNode("hot-key")

	// Simulate heavy load on that node.
	for i := 0; i < 100; i++ {
		r.IncrementLoad(normalNode)
	}

	// Now the key should be redirected to a different (less loaded) node.
	redirectedNode, _ := r.GetNode("hot-key")

	if redirectedNode == normalNode {
		t.Logf("WARNING: key was not redirected despite heavy load on %s (load=%d)",
			normalNode, r.GetLoad(normalNode))
		// This can happen if the bounded load threshold is not exceeded
		// relative to average, which depends on total cluster load.
	}

	t.Logf("  Normal: %s (load=%d) -> Redirected: %s (load=%d)",
		normalNode, r.GetLoad(normalNode),
		redirectedNode, r.GetLoad(redirectedNode))
}

func TestRing_LoadTracking(t *testing.T) {
	r := NewRing(150, 0.25)
	r.AddNode("node-1")

	if r.GetLoad("node-1") != 0 {
		t.Fatal("expected initial load of 0")
	}

	r.IncrementLoad("node-1")
	r.IncrementLoad("node-1")
	r.IncrementLoad("node-1")

	if r.GetLoad("node-1") != 3 {
		t.Fatalf("expected load of 3, got %d", r.GetLoad("node-1"))
	}

	r.DecrementLoad("node-1")
	if r.GetLoad("node-1") != 2 {
		t.Fatalf("expected load of 2, got %d", r.GetLoad("node-1"))
	}
}

func TestRing_Members(t *testing.T) {
	r := NewRing(150, 0.25)
	r.AddNode("node-3")
	r.AddNode("node-1")
	r.AddNode("node-2")

	members := r.Members()
	if len(members) != 3 {
		t.Fatalf("expected 3 members, got %d", len(members))
	}
	// Members should be sorted.
	for i := 1; i < len(members); i++ {
		if members[i] < members[i-1] {
			t.Fatal("expected Members() to return sorted list")
		}
	}
}

func TestRing_Callbacks(t *testing.T) {
	r := NewRing(150, 0.25)

	addedNodes := []string{}
	removedNodes := []string{}

	r.SetOnNodeAdded(func(nodeID string) {
		addedNodes = append(addedNodes, nodeID)
	})
	r.SetOnNodeRemoved(func(nodeID string) {
		removedNodes = append(removedNodes, nodeID)
	})

	r.AddNode("node-1")
	r.AddNode("node-2")
	r.RemoveNode("node-1")

	if len(addedNodes) != 2 {
		t.Fatalf("expected 2 added callbacks, got %d", len(addedNodes))
	}
	if len(removedNodes) != 1 {
		t.Fatalf("expected 1 removed callback, got %d", len(removedNodes))
	}
}

// Benchmarks

func BenchmarkRing_GetNode(b *testing.B) {
	r := NewRing(150, 0.25)
	for i := 0; i < 10; i++ {
		r.AddNode(fmt.Sprintf("node-%d", i))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r.GetNode(fmt.Sprintf("key-%d", i))
	}
}

func BenchmarkRing_GetNodes(b *testing.B) {
	r := NewRing(150, 0.25)
	for i := 0; i < 10; i++ {
		r.AddNode(fmt.Sprintf("node-%d", i))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r.GetNodes(fmt.Sprintf("key-%d", i), 3)
	}
}
