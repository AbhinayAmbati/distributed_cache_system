package client

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	pb "github.com/AbhinayAmbati/distributed_cache_system/api/proto"
	"github.com/AbhinayAmbati/distributed_cache_system/internal/hashing"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Client is the cache client SDK. It understands the hash ring and
// routes requests directly to the correct node.
type Client struct {
	mu    sync.RWMutex
	ring  *hashing.Ring
	conns map[string]*grpc.ClientConn // nodeID → connection
	addrs map[string]string           // nodeID → grpc address

	// L1 client-side cache for hot keys
	l1Cache *L1Cache

	// Options.
	dialTimeout    time.Duration
	requestTimeout time.Duration
}

// Option is a functional option for configuring the Client.
type Option func(*Client)

// WithDialTimeout sets the gRPC dial timeout.
func WithDialTimeout(d time.Duration) Option {
	return func(c *Client) {
		c.dialTimeout = d
	}
}

// WithRequestTimeout sets the default request timeout.
func WithRequestTimeout(d time.Duration) Option {
	return func(c *Client) {
		c.requestTimeout = d
	}
}

// WithL1Cache enables the client-side L1 cache for hot keys.
func WithL1Cache(capacity int, defaultTTL time.Duration) Option {
	return func(c *Client) {
		c.l1Cache = NewL1Cache(capacity, defaultTTL)
	}
}

// NewClient creates a new cache client.
// nodes is a map of nodeID → gRPC address (e.g., "node-1" → "localhost:7001").
func NewClient(nodes map[string]string, opts ...Option) (*Client, error) {
	c := &Client{
		ring:           hashing.NewRing(150, 0.25),
		conns:          make(map[string]*grpc.ClientConn),
		addrs:          make(map[string]string),
		dialTimeout:    5 * time.Second,
		requestTimeout: 3 * time.Second,
	}

	for _, opt := range opts {
		opt(c)
	}

	// Add all nodes to the ring and establish connections.
	for nodeID, addr := range nodes {
		if err := c.addNode(nodeID, addr); err != nil {
			c.Close()
			return nil, fmt.Errorf("connecting to %s (%s): %w", nodeID, addr, err)
		}
	}

	return c, nil
}

// addNode adds a node to the client's ring and establishes a gRPC connection.
func (c *Client) addNode(nodeID, addr string) error {
	ctx, cancel := context.WithTimeout(context.Background(), c.dialTimeout)
	defer cancel()

	conn, err := grpc.DialContext(ctx, addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	)
	if err != nil {
		return err
	}

	c.mu.Lock()
	c.conns[nodeID] = conn
	c.addrs[nodeID] = addr
	c.ring.AddNode(nodeID)
	c.mu.Unlock()

	log.Printf("[client] connected to %s at %s", nodeID, addr)
	return nil
}

// getConn returns the gRPC connection for a key, routed via the hash ring.
func (c *Client) getConn(key string) (*grpc.ClientConn, string, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	nodeID, ok := c.ring.GetNode(key)
	if !ok {
		return nil, "", fmt.Errorf("no nodes available")
	}

	conn, ok := c.conns[nodeID]
	if !ok {
		return nil, "", fmt.Errorf("no connection for node %s", nodeID)
	}

	return conn, nodeID, nil
}

// Get retrieves a value by key from the appropriate cache node.
func (c *Client) Get(ctx context.Context, key string) ([]byte, bool, error) {
	// 1. Check L1 cache first if enabled
	if c.l1Cache != nil {
		if val, ok := c.l1Cache.Get(key); ok {
			return val, true, nil
		}
	}

	conn, _, err := c.getConn(key)
	if err != nil {
		return nil, false, err
	}

	ctx, cancel := context.WithTimeout(ctx, c.requestTimeout)
	defer cancel()

	resp := &pb.GetResponse{}
	err = conn.Invoke(ctx, "/cache.CacheService/Get", &pb.GetRequest{Key: key}, resp)
	if err != nil {
		return nil, false, fmt.Errorf("get %q: %w", key, err)
	}

	// 2. If it's a hot key and L1 cache is enabled, store in L1
	if resp.Found && resp.IsHot && c.l1Cache != nil {
		// Use remaining TTL from response, or fallback to default
		ttl := time.Duration(resp.TtlMs) * time.Millisecond
		c.l1Cache.Set(key, resp.Value, ttl)
	}

	return resp.Value, resp.Found, nil
}

// Set stores a key-value pair with optional TTL (0 = no expiry).
func (c *Client) Set(ctx context.Context, key string, value []byte, ttl time.Duration) error {
	// Invalidate L1 cache to maintain consistency
	if c.l1Cache != nil {
		c.l1Cache.Delete(key)
	}

	conn, _, err := c.getConn(key)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(ctx, c.requestTimeout)
	defer cancel()

	resp := &pb.SetResponse{}
	err = conn.Invoke(ctx, "/cache.CacheService/Set", &pb.SetRequest{
		Key:   key,
		Value: value,
		TtlMs: ttl.Milliseconds(),
	}, resp)
	if err != nil {
		return fmt.Errorf("set %q: %w", key, err)
	}

	return nil
}

// Delete removes a key from the cache.
func (c *Client) Delete(ctx context.Context, key string) (bool, error) {
	// Invalidate L1 cache
	if c.l1Cache != nil {
		c.l1Cache.Delete(key)
	}

	conn, _, err := c.getConn(key)
	if err != nil {
		return false, err
	}

	ctx, cancel := context.WithTimeout(ctx, c.requestTimeout)
	defer cancel()

	resp := &pb.DeleteResponse{}
	err = conn.Invoke(ctx, "/cache.CacheService/Delete", &pb.DeleteRequest{Key: key}, resp)
	if err != nil {
		return false, fmt.Errorf("delete %q: %w", key, err)
	}

	return resp.Existed, nil
}

// Ping checks if a specific node is alive.
func (c *Client) Ping(ctx context.Context, nodeID string) (*pb.PingResponse, error) {
	c.mu.RLock()
	conn, ok := c.conns[nodeID]
	c.mu.RUnlock()

	if !ok {
		return nil, fmt.Errorf("unknown node %s", nodeID)
	}

	ctx, cancel := context.WithTimeout(ctx, c.requestTimeout)
	defer cancel()

	resp := &pb.PingResponse{}
	err := conn.Invoke(ctx, "/cache.CacheService/Ping", &pb.PingRequest{}, resp)
	if err != nil {
		return nil, fmt.Errorf("ping %s: %w", nodeID, err)
	}

	return resp, nil
}

// NodeForKey returns the node ID that owns a given key (for debugging).
func (c *Client) NodeForKey(key string) (string, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	nodeID, ok := c.ring.GetNode(key)
	if !ok {
		return "", fmt.Errorf("no nodes available")
	}
	return nodeID, nil
}

// Nodes returns the list of connected node IDs.
func (c *Client) Nodes() []string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.ring.Members()
}

// Close closes all connections to cache nodes.
func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	var firstErr error
	for nodeID, conn := range c.conns {
		if err := conn.Close(); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("closing connection to %s: %w", nodeID, err)
		}
	}
	c.conns = make(map[string]*grpc.ClientConn)

	if c.l1Cache != nil {
		c.l1Cache.Clear()
	}

	return firstErr
}
