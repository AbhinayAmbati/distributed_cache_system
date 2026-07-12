package client

import (
	"testing"
	"time"
)

func TestL1Cache_Basic(t *testing.T) {
	cache := NewL1Cache(2, 50*time.Millisecond)

	// Set key
	cache.Set("key1", []byte("val1"), 0)
	val, ok := cache.Get("key1")
	if !ok || string(val) != "val1" {
		t.Fatalf("expected to get val1, got %s (ok=%t)", string(val), ok)
	}

	// Wait for TTL
	time.Sleep(100 * time.Millisecond)
	_, ok = cache.Get("key1")
	if ok {
		t.Fatal("expected key1 to expire")
	}
}

func TestL1Cache_Eviction(t *testing.T) {
	cache := NewL1Cache(2, 10*time.Second)

	cache.Set("k1", []byte("v1"), 0)
	cache.Set("k2", []byte("v2"), 0)
	cache.Set("k3", []byte("v3"), 0) // Should evict k1 (LRU)

	_, ok := cache.Get("k1")
	if ok {
		t.Fatal("expected k1 to be evicted")
	}

	// Update order
	cache.Get("k2")                  // Move k2 to front
	cache.Set("k4", []byte("v4"), 0) // Should evict k3

	_, ok = cache.Get("k3")
	if ok {
		t.Fatal("expected k3 to be evicted")
	}
	_, ok = cache.Get("k2")
	if !ok {
		t.Fatal("expected k2 to still exist")
	}
}

func TestL1Cache_DeleteAndClear(t *testing.T) {
	cache := NewL1Cache(5, 10*time.Second)
	cache.Set("k1", []byte("v1"), 0)
	cache.Set("k2", []byte("v2"), 0)

	cache.Delete("k1")
	_, ok := cache.Get("k1")
	if ok {
		t.Fatal("expected k1 to be deleted")
	}

	cache.Clear()
	if cache.Len() != 0 {
		t.Fatalf("expected len 0 after clear, got %d", cache.Len())
	}
}


