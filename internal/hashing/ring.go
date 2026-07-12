package hashing

import (
	"fmt"
	"hash/fnv"
	"sort"
	"sync"
	"sync/atomic"
)

// VNode represents a virtual node on the consistent hash ring.
type VNode struct {
	Hash   uint64
	NodeID string
	Index  int // Virtual node index for this physical node.
}

// NodeLoad tracks the current load on a physical node.
type NodeLoad struct {
	NodeID string
	Active atomic.Int64
}

// Ring implements consistent hashing with bounded loads.
// It distributes keys across nodes using virtual nodes on a hash ring,
// and enforces a maximum load factor to prevent any single node from
// being overloaded (Google's "Consistent Hashing with Bounded Loads").
type Ring struct {
	mu            sync.RWMutex
	vnodes        []VNode         // Sorted by hash.
	nodeVnodes    int             // Number of virtual nodes per physical node.
	nodes         map[string]bool // Set of physical node IDs.
	loads         map[string]*NodeLoad
	maxLoadFactor float64         // ε: max load = (1+ε) × avg load.

	// Callbacks for ring changes.
	onNodeAdded   func(nodeID string)
	onNodeRemoved func(nodeID string)
}

// NewRing creates a new consistent hash ring.
// vnodeCount is the number of virtual nodes per physical node.
// maxLoadFactor is ε for bounded loads (e.g., 0.25 means max 125% of avg).
func NewRing(vnodeCount int, maxLoadFactor float64) *Ring {
	if vnodeCount <= 0 {
		vnodeCount = 150
	}
	if maxLoadFactor < 0 {
		maxLoadFactor = 0.25
	}

	return &Ring{
		vnodes:        make([]VNode, 0),
		nodeVnodes:    vnodeCount,
		nodes:         make(map[string]bool),
		loads:         make(map[string]*NodeLoad),
		maxLoadFactor: maxLoadFactor,
	}
}

// SetOnNodeAdded sets a callback invoked when a node is added to the ring.
func (r *Ring) SetOnNodeAdded(fn func(nodeID string)) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.onNodeAdded = fn
}

// SetOnNodeRemoved sets a callback invoked when a node is removed from the ring.
func (r *Ring) SetOnNodeRemoved(fn func(nodeID string)) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.onNodeRemoved = fn
}

// AddNode adds a physical node to the ring with its virtual nodes.
// Returns false if the node already exists.
func (r *Ring) AddNode(nodeID string) bool {
	r.mu.Lock()

	if r.nodes[nodeID] {
		r.mu.Unlock()
		return false
	}

	r.nodes[nodeID] = true
	r.loads[nodeID] = &NodeLoad{NodeID: nodeID}

	// Generate virtual nodes.
	for i := 0; i < r.nodeVnodes; i++ {
		hash := hashVNode(nodeID, i)
		r.vnodes = append(r.vnodes, VNode{
			Hash:   hash,
			NodeID: nodeID,
			Index:  i,
		})
	}

	// Re-sort the ring.
	sort.Slice(r.vnodes, func(i, j int) bool {
		return r.vnodes[i].Hash < r.vnodes[j].Hash
	})

	callback := r.onNodeAdded
	r.mu.Unlock()

	if callback != nil {
		callback(nodeID)
	}
	return true
}

// RemoveNode removes a physical node and all its virtual nodes from the ring.
// Returns false if the node doesn't exist.
func (r *Ring) RemoveNode(nodeID string) bool {
	r.mu.Lock()

	if !r.nodes[nodeID] {
		r.mu.Unlock()
		return false
	}

	delete(r.nodes, nodeID)
	delete(r.loads, nodeID)

	// Remove all virtual nodes for this physical node.
	filtered := make([]VNode, 0, len(r.vnodes)-r.nodeVnodes)
	for _, vn := range r.vnodes {
		if vn.NodeID != nodeID {
			filtered = append(filtered, vn)
		}
	}
	r.vnodes = filtered

	callback := r.onNodeRemoved
	r.mu.Unlock()

	if callback != nil {
		callback(nodeID)
	}
	return true
}

// GetNode returns the node responsible for the given key.
// It implements bounded loads: if the target node is overloaded,
// it moves clockwise to the next eligible node.
func (r *Ring) GetNode(key string) (string, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if len(r.vnodes) == 0 {
		return "", false
	}

	hash := hashKey(key)
	idx := r.search(hash)

	avgLoad := r.averageLoad()
	maxLoad := int64(float64(avgLoad) * (1 + r.maxLoadFactor))
	if maxLoad < 1 {
		maxLoad = 1
	}

	// Walk clockwise from the target position to find a non-overloaded node.
	n := len(r.vnodes)
	for i := 0; i < n; i++ {
		vnode := r.vnodes[(idx+i)%n]
		load := r.loads[vnode.NodeID]
		if load == nil || load.Active.Load() < maxLoad {
			return vnode.NodeID, true
		}
	}

	// All nodes overloaded — return the original target anyway.
	return r.vnodes[idx].NodeID, true
}

// GetNodes returns up to n distinct nodes responsible for the given key,
// walking clockwise from the key's position on the ring.
// Used for replication: first node = primary, rest = secondaries.
func (r *Ring) GetNodes(key string, n int) []string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if len(r.vnodes) == 0 {
		return nil
	}

	nodeCount := len(r.nodes)
	if n > nodeCount {
		n = nodeCount
	}

	hash := hashKey(key)
	idx := r.search(hash)

	result := make([]string, 0, n)
	seen := make(map[string]bool)

	total := len(r.vnodes)
	for i := 0; i < total && len(result) < n; i++ {
		vnode := r.vnodes[(idx+i)%total]
		if !seen[vnode.NodeID] {
			seen[vnode.NodeID] = true
			result = append(result, vnode.NodeID)
		}
	}

	return result
}

// IncrementLoad atomically increments the load for a node.
// Call this when a request starts being processed by the node.
func (r *Ring) IncrementLoad(nodeID string) {
	r.mu.RLock()
	load := r.loads[nodeID]
	r.mu.RUnlock()

	if load != nil {
		load.Active.Add(1)
	}
}

// DecrementLoad atomically decrements the load for a node.
// Call this when a request finishes being processed by the node.
func (r *Ring) DecrementLoad(nodeID string) {
	r.mu.RLock()
	load := r.loads[nodeID]
	r.mu.RUnlock()

	if load != nil {
		load.Active.Add(-1)
	}
}

// GetLoad returns the current load for a node.
func (r *Ring) GetLoad(nodeID string) int64 {
	r.mu.RLock()
	load := r.loads[nodeID]
	r.mu.RUnlock()

	if load != nil {
		return load.Active.Load()
	}
	return 0
}

// Members returns the list of all physical node IDs in the ring.
func (r *Ring) Members() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	members := make([]string, 0, len(r.nodes))
	for id := range r.nodes {
		members = append(members, id)
	}
	sort.Strings(members)
	return members
}

// Size returns the number of physical nodes in the ring.
func (r *Ring) Size() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.nodes)
}

// IsEmpty returns true if the ring has no nodes.
func (r *Ring) IsEmpty() bool {
	return r.Size() == 0
}

// HasNode checks if a node exists in the ring.
func (r *Ring) HasNode(nodeID string) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.nodes[nodeID]
}

// --- Internal helpers ---

// search returns the index of the first vnode with hash >= target.
// Uses binary search on the sorted vnodes slice.
func (r *Ring) search(hash uint64) int {
	idx := sort.Search(len(r.vnodes), func(i int) bool {
		return r.vnodes[i].Hash >= hash
	})
	// Wrap around if we've gone past the end.
	if idx >= len(r.vnodes) {
		idx = 0
	}
	return idx
}

// averageLoad returns the average load across all nodes.
// Must be called with r.mu held (at least RLock).
func (r *Ring) averageLoad() float64 {
	n := len(r.nodes)
	if n == 0 {
		return 0
	}

	var total int64
	for _, load := range r.loads {
		total += load.Active.Load()
	}
	return float64(total) / float64(n)
}

// hashKey hashes a cache key to a position on the ring.
func hashKey(key string) uint64 {
	h := fnv.New64a()
	h.Write([]byte(key))
	return h.Sum64()
}

// hashVNode generates a hash for a virtual node.
// Format: "nodeID#index" — gives each vnode a unique position.
func hashVNode(nodeID string, index int) uint64 {
	h := fnv.New64a()
	h.Write([]byte(fmt.Sprintf("%s#%d", nodeID, index)))
	return h.Sum64()
}
