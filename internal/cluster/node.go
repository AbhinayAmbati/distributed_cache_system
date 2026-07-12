package cluster

import (
	"sync"
	"time"
)

// NodeStatus represents the health state of a node.
type NodeStatus int

const (
	// StatusAlive means the node is healthy and responding.
	StatusAlive NodeStatus = iota
	// StatusSuspect means the node may be failing (SWIM protocol).
	StatusSuspect
	// StatusDead means the node is confirmed unreachable.
	StatusDead
	// StatusLeft means the node gracefully left the cluster.
	StatusLeft
)

// String returns a human-readable status name.
func (s NodeStatus) String() string {
	switch s {
	case StatusAlive:
		return "ALIVE"
	case StatusSuspect:
		return "SUSPECT"
	case StatusDead:
		return "DEAD"
	case StatusLeft:
		return "LEFT"
	default:
		return "UNKNOWN"
	}
}

// NodeInfo holds metadata about a node in the cluster.
type NodeInfo struct {
	mu sync.RWMutex

	// ID is the unique identifier for this node.
	ID string

	// GRPCAddr is the address for gRPC communication (e.g., "host:7001").
	GRPCAddr string

	// GossipAddr is the address for gossip protocol communication.
	GossipAddr string

	// HTTPAddr is the address for the HTTP admin server.
	HTTPAddr string

	// Status is the current health status of this node.
	Status NodeStatus

	// Incarnation is a monotonically increasing counter used by SWIM
	// to distinguish between stale and current status updates.
	Incarnation uint64

	// JoinedAt is when this node joined the cluster.
	JoinedAt time.Time

	// LastSeen is the last time we received communication from this node.
	LastSeen time.Time
}

// NewNodeInfo creates a new NodeInfo with ALIVE status.
func NewNodeInfo(id, grpcAddr, gossipAddr, httpAddr string) *NodeInfo {
	now := time.Now()
	return &NodeInfo{
		ID:          id,
		GRPCAddr:    grpcAddr,
		GossipAddr:  gossipAddr,
		HTTPAddr:    httpAddr,
		Status:      StatusAlive,
		Incarnation: 0,
		JoinedAt:    now,
		LastSeen:    now,
	}
}

// GetStatus returns the current node status (thread-safe).
func (n *NodeInfo) GetStatus() NodeStatus {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.Status
}

// SetStatus updates the node status (thread-safe).
func (n *NodeInfo) SetStatus(status NodeStatus) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.Status = status
}

// Touch updates the LastSeen timestamp.
func (n *NodeInfo) Touch() {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.LastSeen = time.Now()
}

// IsAlive returns true if the node is in ALIVE status.
func (n *NodeInfo) IsAlive() bool {
	return n.GetStatus() == StatusAlive
}

// Clone returns a deep copy of the NodeInfo.
func (n *NodeInfo) Clone() *NodeInfo {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return &NodeInfo{
		ID:          n.ID,
		GRPCAddr:    n.GRPCAddr,
		GossipAddr:  n.GossipAddr,
		HTTPAddr:    n.HTTPAddr,
		Status:      n.Status,
		Incarnation: n.Incarnation,
		JoinedAt:    n.JoinedAt,
		LastSeen:    n.LastSeen,
	}
}

// NodeRegistry manages the set of known nodes in the cluster.
type NodeRegistry struct {
	mu    sync.RWMutex
	nodes map[string]*NodeInfo
	self  string // ID of the local node.
}

// NewNodeRegistry creates a new node registry.
func NewNodeRegistry(selfID string) *NodeRegistry {
	return &NodeRegistry{
		nodes: make(map[string]*NodeInfo),
		self:  selfID,
	}
}

// Register adds or updates a node in the registry.
func (nr *NodeRegistry) Register(info *NodeInfo) {
	nr.mu.Lock()
	defer nr.mu.Unlock()
	nr.nodes[info.ID] = info
}

// Remove removes a node from the registry.
func (nr *NodeRegistry) Remove(nodeID string) {
	nr.mu.Lock()
	defer nr.mu.Unlock()
	delete(nr.nodes, nodeID)
}

// Get returns the NodeInfo for a given node ID.
func (nr *NodeRegistry) Get(nodeID string) (*NodeInfo, bool) {
	nr.mu.RLock()
	defer nr.mu.RUnlock()
	info, ok := nr.nodes[nodeID]
	return info, ok
}

// GetAll returns a copy of all registered nodes.
func (nr *NodeRegistry) GetAll() []*NodeInfo {
	nr.mu.RLock()
	defer nr.mu.RUnlock()

	result := make([]*NodeInfo, 0, len(nr.nodes))
	for _, info := range nr.nodes {
		result = append(result, info.Clone())
	}
	return result
}

// GetAlive returns all nodes with ALIVE status.
func (nr *NodeRegistry) GetAlive() []*NodeInfo {
	nr.mu.RLock()
	defer nr.mu.RUnlock()

	result := make([]*NodeInfo, 0)
	for _, info := range nr.nodes {
		if info.GetStatus() == StatusAlive {
			result = append(result, info.Clone())
		}
	}
	return result
}

// Size returns the number of registered nodes.
func (nr *NodeRegistry) Size() int {
	nr.mu.RLock()
	defer nr.mu.RUnlock()
	return len(nr.nodes)
}

// SelfID returns the local node's ID.
func (nr *NodeRegistry) SelfID() string {
	return nr.self
}
