package cluster

import (
	"context"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	pb "github.com/AbhinayAmbati/distributed_cache_system/api/proto"
	"github.com/AbhinayAmbati/distributed_cache_system/internal/hashing"
	"github.com/AbhinayAmbati/distributed_cache_system/internal/store"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// ReplicationManager handles replicating writes to secondary nodes
// and coordinating failover when a primary becomes unavailable.
type ReplicationManager struct {
	mu sync.RWMutex

	selfID       string
	store        *store.Store
	ring         *hashing.Ring
	registry     *NodeRegistry
	replicaCount int

	// Monotonically increasing sequence number for ordering replicated writes.
	sequence atomic.Uint64

	// gRPC connections to peer nodes for replication.
	peerConns map[string]*grpc.ClientConn

	// Channel to signal replication events (for testing/monitoring).
	replicationEvents chan ReplicationEvent

	dialTimeout time.Duration
}

// ReplicationEvent represents a replication operation that occurred.
type ReplicationEvent struct {
	Key       string
	Operation pb.Operation
	Target    string // Target node ID.
	Success   bool
	Timestamp time.Time
}

// NewReplicationManager creates a new replication manager.
func NewReplicationManager(
	selfID string,
	s *store.Store,
	ring *hashing.Ring,
	registry *NodeRegistry,
	replicaCount int,
) *ReplicationManager {
	if replicaCount < 1 {
		replicaCount = 3
	}

	return &ReplicationManager{
		selfID:            selfID,
		store:             s,
		ring:              ring,
		registry:          registry,
		replicaCount:      replicaCount,
		peerConns:         make(map[string]*grpc.ClientConn),
		replicationEvents: make(chan ReplicationEvent, 1000),
		dialTimeout:       3 * time.Second,
	}
}

// ReplicateWrite asynchronously replicates a Set operation to secondary nodes.
// This is called by the primary node after a successful local write.
func (rm *ReplicationManager) ReplicateWrite(key string, value []byte, ttlMs int64) {
	replicas := rm.getSecondaries(key)
	if len(replicas) == 0 {
		return
	}

	seq := rm.sequence.Add(1)

	req := &pb.ReplicateRequest{
		Key:        key,
		Value:      value,
		TtlMs:      ttlMs,
		Sequence:   seq,
		SourceNode: rm.selfID,
		Op:         pb.OpSet,
	}

	// Replicate asynchronously to each secondary.
	for _, nodeID := range replicas {
		go rm.sendReplicate(nodeID, req)
	}
}

// ReplicateDelete asynchronously replicates a Delete operation to secondaries.
func (rm *ReplicationManager) ReplicateDelete(key string) {
	replicas := rm.getSecondaries(key)
	if len(replicas) == 0 {
		return
	}

	seq := rm.sequence.Add(1)

	req := &pb.ReplicateRequest{
		Key:        key,
		Sequence:   seq,
		SourceNode: rm.selfID,
		Op:         pb.OpDelete,
	}

	for _, nodeID := range replicas {
		go rm.sendReplicate(nodeID, req)
	}
}

// IsPrimary returns true if this node is the primary for the given key.
func (rm *ReplicationManager) IsPrimary(key string) bool {
	nodes := rm.ring.GetNodes(key, 1)
	return len(nodes) > 0 && nodes[0] == rm.selfID
}

// IsReplica returns true if this node is in the replica set for the given key.
func (rm *ReplicationManager) IsReplica(key string) bool {
	nodes := rm.ring.GetNodes(key, rm.replicaCount)
	for _, n := range nodes {
		if n == rm.selfID {
			return true
		}
	}
	return false
}

// GetPrimary returns the primary node ID for a key.
func (rm *ReplicationManager) GetPrimary(key string) (string, bool) {
	nodes := rm.ring.GetNodes(key, 1)
	if len(nodes) == 0 {
		return "", false
	}
	return nodes[0], true
}

// GetReplicaSet returns the full replica set for a key.
func (rm *ReplicationManager) GetReplicaSet(key string) []string {
	return rm.ring.GetNodes(key, rm.replicaCount)
}

// HandleNodeFailure is called when a node is detected as dead.
// It triggers re-replication for keys that were replicated on the dead node.
func (rm *ReplicationManager) HandleNodeFailure(deadNodeID string) {
	log.Printf("[replication] handling failure of node %s", deadNodeID)

	// Close connection to dead node.
	rm.mu.Lock()
	if conn, ok := rm.peerConns[deadNodeID]; ok {
		conn.Close()
		delete(rm.peerConns, deadNodeID)
	}
	rm.mu.Unlock()

	// Re-replicate our keys that had the dead node as a replica.
	rm.reReplicateForDeadNode(deadNodeID)
}

// ConnectToPeer establishes a gRPC connection to a peer node.
func (rm *ReplicationManager) ConnectToPeer(nodeID, addr string) error {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	// Skip if already connected.
	if _, ok := rm.peerConns[nodeID]; ok {
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), rm.dialTimeout)
	defer cancel()

	conn, err := grpc.DialContext(ctx, addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return fmt.Errorf("dialing %s at %s: %w", nodeID, addr, err)
	}

	rm.peerConns[nodeID] = conn
	log.Printf("[replication] connected to peer %s at %s", nodeID, addr)
	return nil
}

// DisconnectPeer closes the connection to a peer node.
func (rm *ReplicationManager) DisconnectPeer(nodeID string) {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	if conn, ok := rm.peerConns[nodeID]; ok {
		conn.Close()
		delete(rm.peerConns, nodeID)
	}
}

// Events returns the channel of replication events (for monitoring).
func (rm *ReplicationManager) Events() <-chan ReplicationEvent {
	return rm.replicationEvents
}

// Close cleans up all peer connections.
func (rm *ReplicationManager) Close() {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	for nodeID, conn := range rm.peerConns {
		conn.Close()
		delete(rm.peerConns, nodeID)
	}
	close(rm.replicationEvents)
}

// --- Internal helpers ---

// getSecondaries returns the list of secondary node IDs for a key
// (i.e., all replica nodes except self).
func (rm *ReplicationManager) getSecondaries(key string) []string {
	allReplicas := rm.ring.GetNodes(key, rm.replicaCount)

	secondaries := make([]string, 0, len(allReplicas)-1)
	for _, nodeID := range allReplicas {
		if nodeID != rm.selfID {
			secondaries = append(secondaries, nodeID)
		}
	}
	return secondaries
}

// sendReplicate sends a replication request to a specific node.
func (rm *ReplicationManager) sendReplicate(nodeID string, req *pb.ReplicateRequest) {
	rm.mu.RLock()
	conn, ok := rm.peerConns[nodeID]
	rm.mu.RUnlock()

	event := ReplicationEvent{
		Key:       req.Key,
		Operation: req.Op,
		Target:    nodeID,
		Timestamp: time.Now(),
	}

	if !ok {
		log.Printf("[replication] no connection to %s, skipping replication for key %s",
			nodeID, req.Key)
		event.Success = false
		rm.emitEvent(event)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	resp := &pb.ReplicateResponse{}
	err := conn.Invoke(ctx, "/cache.CacheService/Replicate", req, resp)
	if err != nil {
		log.Printf("[replication] failed to replicate key %s to %s: %v",
			req.Key, nodeID, err)
		event.Success = false
	} else {
		event.Success = true
	}

	rm.emitEvent(event)
}

// emitEvent sends a replication event to the events channel (non-blocking).
func (rm *ReplicationManager) emitEvent(event ReplicationEvent) {
	select {
	case rm.replicationEvents <- event:
	default:
		// Channel full, drop the event.
	}
}

// reReplicateForDeadNode finds keys that need new replicas after a node failure.
func (rm *ReplicationManager) reReplicateForDeadNode(deadNodeID string) {
	count := 0

	rm.store.ForEach(func(key string, value []byte, ttl time.Duration) bool {
		// Check if we are the new primary for this key.
		if rm.IsPrimary(key) {
			// Check if the dead node was in the old replica set.
			// The ring has already been updated (dead node removed),
			// so the new replica set excludes the dead node.
			// We just need to replicate to the new replica set members.
			rm.ReplicateWrite(key, value, ttl.Milliseconds())
			count++
		}
		return true
	})

	log.Printf("[replication] re-replicated %d keys after failure of %s", count, deadNodeID)
}
