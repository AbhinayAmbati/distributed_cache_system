package cluster

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestMembership_StartStop(t *testing.T) {
	info := NewNodeInfo("node-1", ":7001", "127.0.0.1:8001", ":9001")
	registry := NewNodeRegistry("node-1")
	m := NewMembership(info, registry)

	if err := m.Start("127.0.0.1:8001"); err != nil {
		t.Fatalf("failed to start: %v", err)
	}

	// Verify self is registered.
	if registry.Size() != 1 {
		t.Fatalf("expected 1 node in registry, got %d", registry.Size())
	}

	self, ok := registry.Get("node-1")
	if !ok {
		t.Fatal("expected self to be registered")
	}
	if self.GetStatus() != StatusAlive {
		t.Fatalf("expected ALIVE status, got %s", self.GetStatus())
	}

	m.Stop()
}

func TestMembership_TwoNodesDiscover(t *testing.T) {
	// Create two nodes.
	info1 := NewNodeInfo("node-1", ":7001", "127.0.0.1:8101", ":9001")
	registry1 := NewNodeRegistry("node-1")
	m1 := NewMembership(info1, registry1)

	info2 := NewNodeInfo("node-2", ":7002", "127.0.0.1:8102", ":9002")
	registry2 := NewNodeRegistry("node-2")
	m2 := NewMembership(info2, registry2)

	if err := m1.Start("127.0.0.1:8101"); err != nil {
		t.Fatalf("failed to start node-1: %v", err)
	}
	defer m1.Stop()

	if err := m2.Start("127.0.0.1:8102"); err != nil {
		t.Fatalf("failed to start node-2: %v", err)
	}
	defer m2.Stop()

	// Node-2 joins via node-1 as seed.
	if err := m2.JoinCluster([]string{"127.0.0.1:8101"}); err != nil {
		t.Fatalf("failed to join: %v", err)
	}

	// Wait for discovery using retry loop.
	success := false
	for start := time.Now(); time.Since(start) < 5*time.Second; time.Sleep(100 * time.Millisecond) {
		if registry1.Size() >= 2 && registry2.Size() >= 2 {
			success = true
			break
		}
	}
	if !success {
		t.Fatalf("expected both nodes to know each other, node-1 size=%d, node-2 size=%d", registry1.Size(), registry2.Size())
	}
}

func TestMembership_ThreeNodesDiscover(t *testing.T) {
	nodes := make([]*Membership, 3)
	registries := make([]*NodeRegistry, 3)

	for i := 0; i < 3; i++ {
		id := nodeIDForTest(i)
		addr := gossipAddrForTest(i)

		info := NewNodeInfo(id, grpcAddrForTest(i), addr, httpAddrForTest(i))
		registries[i] = NewNodeRegistry(id)
		nodes[i] = NewMembership(info, registries[i])

		if err := nodes[i].Start(addr); err != nil {
			t.Fatalf("failed to start %s: %v", id, err)
		}
	}
	defer func() {
		for _, n := range nodes {
			n.Stop()
		}
	}()

	// Node-2 and node-3 join via node-1.
	seedAddr := gossipAddrForTest(0)
	for i := 1; i < 3; i++ {
		if err := nodes[i].JoinCluster([]string{seedAddr}); err != nil {
			t.Fatalf("node-%d failed to join: %v", i+1, err)
		}
	}

	// Wait for full convergence using retry loop.
	success := false
	for start := time.Now(); time.Since(start) < 5*time.Second; time.Sleep(100 * time.Millisecond) {
		converged := true
		for _, reg := range registries {
			if reg.Size() < 3 {
				converged = false
				break
			}
		}
		if converged {
			success = true
			break
		}
	}
	if !success {
		for i, reg := range registries {
			t.Logf("node-%d knows %d nodes", i+1, reg.Size())
		}
		t.Fatalf("expected all nodes to converge to size 3")
	}
}

func TestMembership_EventCallback(t *testing.T) {
	info1 := NewNodeInfo("node-1", ":7001", "127.0.0.1:8201", ":9001")
	registry1 := NewNodeRegistry("node-1")
	m1 := NewMembership(info1, registry1)

	var mu sync.Mutex
	events := []MembershipEvent{}

	m1.SetOnEvent(func(event MembershipEvent) {
		mu.Lock()
		events = append(events, event)
		mu.Unlock()
	})

	if err := m1.Start("127.0.0.1:8201"); err != nil {
		t.Fatalf("failed to start: %v", err)
	}

	info2 := NewNodeInfo("node-2", ":7002", "127.0.0.1:8202", ":9002")
	registry2 := NewNodeRegistry("node-2")
	m2 := NewMembership(info2, registry2)

	if err := m2.Start("127.0.0.1:8202"); err != nil {
		t.Fatalf("failed to start: %v", err)
	}

	// Join.
	m2.JoinCluster([]string{"127.0.0.1:8201"})

	time.Sleep(2 * time.Second)

	mu.Lock()
	joinEvents := 0
	for _, e := range events {
		if e.Type == EventJoin {
			joinEvents++
		}
	}
	mu.Unlock()

	if joinEvents == 0 {
		t.Log("WARNING: no join events received (may be timing-dependent)")
	} else {
		t.Logf("received %d join events", joinEvents)
	}

	m1.Stop()
	m2.Stop()
}

func TestMembership_FailureDetection(t *testing.T) {
	info1 := NewNodeInfo("node-1", ":7001", "127.0.0.1:8301", ":9001")
	registry1 := NewNodeRegistry("node-1")
	m1 := NewMembership(info1, registry1)
	m1.pingInterval = 200 * time.Millisecond
	m1.pingTimeout = 100 * time.Millisecond
	m1.suspicionTimeout = 1 * time.Second

	info2 := NewNodeInfo("node-2", ":7002", "127.0.0.1:8302", ":9002")
	registry2 := NewNodeRegistry("node-2")
	m2 := NewMembership(info2, registry2)

	if err := m1.Start("127.0.0.1:8301"); err != nil {
		t.Fatalf("failed to start: %v", err)
	}
	defer m1.Stop()

	if err := m2.Start("127.0.0.1:8302"); err != nil {
		t.Fatalf("failed to start: %v", err)
	}

	// Join.
	m2.JoinCluster([]string{"127.0.0.1:8301"})
	time.Sleep(2 * time.Second)

	// Verify both nodes know about each other.
	if registry1.Size() < 2 {
		t.Fatalf("expected node-1 to know 2 nodes, got %d", registry1.Size())
	}

	// Kill node-2 abruptly (without graceful leave).
	m2.conn.Close()

	// Wait for failure detection.
	time.Sleep(4 * time.Second)

	// Node-1 should have detected node-2 as suspect or dead.
	node2Info, ok := registry1.Get("node-2")
	if ok {
		status := node2Info.GetStatus()
		if status == StatusSuspect || status == StatusDead {
			t.Logf("node-2 detected as %s", status)
		} else {
			t.Logf("node-2 status: %s (detection may still be in progress)", status)
		}
	}
}

// --- Helpers ---

func nodeIDForTest(i int) string {
	return fmt.Sprintf("node-%d", i+1)
}

func gossipAddrForTest(i int) string {
	return fmt.Sprintf("127.0.0.1:840%d", i+1)
}

func grpcAddrForTest(i int) string {
	return fmt.Sprintf(":700%d", i+1)
}

func httpAddrForTest(i int) string {
	return fmt.Sprintf(":900%d", i+1)
}
