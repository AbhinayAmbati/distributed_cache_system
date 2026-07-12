package client

import (
	"container/list"
	"sync"
	"time"
)

// l1Entry holds the cached value and its expiration time.
type l1Entry struct {
	key       string
	value     []byte
	expiresAt time.Time
}

// L1Cache is a simple thread-safe LRU cache with TTL support for client-side hot key caching.
type L1Cache struct {
	mu         sync.RWMutex
	capacity   int
	evictList  *list.List
	items      map[string]*list.Element
	defaultTTL time.Duration
}

// NewL1Cache creates a new L1 cache with the specified capacity and default TTL.
func NewL1Cache(capacity int, defaultTTL time.Duration) *L1Cache {
	if capacity <= 0 {
		capacity = 1000
	}
	if defaultTTL <= 0 {
		defaultTTL = 5 * time.Second
	}
	return &L1Cache{
		capacity:   capacity,
		evictList:  list.New(),
		items:      make(map[string]*list.Element),
		defaultTTL: defaultTTL,
	}
}

// Get retrieves a key's value if it exists and is not expired.
func (c *L1Cache) Get(key string) ([]byte, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	elem, exists := c.items[key]
	if !exists {
		return nil, false
	}

	entry := elem.Value.(*l1Entry)
	if time.Now().After(entry.expiresAt) {
		c.removeElement(elem)
		return nil, false
	}

	c.evictList.MoveToFront(elem)
	
	// Copy value to prevent external modification
	valCopy := make([]byte, len(entry.value))
	copy(valCopy, entry.value)
	return valCopy, true
}

// Set adds or updates a key-value pair in the L1 cache.
func (c *L1Cache) Set(key string, value []byte, ttl time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if ttl <= 0 {
		ttl = c.defaultTTL
	}

	expiresAt := time.Now().Add(ttl)

	// If key exists, update value and move to front
	if elem, ok := c.items[key]; ok {
		c.evictList.MoveToFront(elem)
		entry := elem.Value.(*l1Entry)
		entry.value = make([]byte, len(value))
		copy(entry.value, value)
		entry.expiresAt = expiresAt
		return
	}

	// Add new item
	entry := &l1Entry{
		key:       key,
		value:     make([]byte, len(value)),
		expiresAt: expiresAt,
	}
	copy(entry.value, value)

	elem := c.evictList.PushFront(entry)
	c.items[key] = elem

	// Evict oldest if capacity exceeded
	if c.evictList.Len() > c.capacity {
		c.evictOldest()
	}
}

// Delete removes a key from the L1 cache.
func (c *L1Cache) Delete(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if elem, ok := c.items[key]; ok {
		c.removeElement(elem)
	}
}

// Clear clears all items from the cache.
func (c *L1Cache) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.evictList.Init()
	c.items = make(map[string]*list.Element)
}

// Len returns the number of items in the L1 cache.
func (c *L1Cache) Len() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.evictList.Len()
}

func (c *L1Cache) evictOldest() {
	elem := c.evictList.Back()
	if elem != nil {
		c.removeElement(elem)
	}
}

func (c *L1Cache) removeElement(elem *list.Element) {
	c.evictList.Remove(elem)
	entry := elem.Value.(*l1Entry)
	delete(c.items, entry.key)
}
