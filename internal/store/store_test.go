package store

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestStore_SetAndGet(t *testing.T) {
	s := New()

	// Set a value.
	s.Set("key1", []byte("value1"), 0)

	// Get should return the value.
	val, ok := s.Get("key1")
	if !ok {
		t.Fatal("expected key1 to exist")
	}
	if string(val) != "value1" {
		t.Fatalf("expected 'value1', got '%s'", string(val))
	}
}

func TestStore_GetMissing(t *testing.T) {
	s := New()

	val, ok := s.Get("nonexistent")
	if ok {
		t.Fatal("expected key to not exist")
	}
	if val != nil {
		t.Fatal("expected nil value for missing key")
	}
}

func TestStore_Overwrite(t *testing.T) {
	s := New()

	s.Set("key1", []byte("v1"), 0)
	s.Set("key1", []byte("v2"), 0)

	val, ok := s.Get("key1")
	if !ok {
		t.Fatal("expected key1 to exist")
	}
	if string(val) != "v2" {
		t.Fatalf("expected 'v2', got '%s'", string(val))
	}
}

func TestStore_Delete(t *testing.T) {
	s := New()

	s.Set("key1", []byte("value1"), 0)

	if !s.Delete("key1") {
		t.Fatal("expected Delete to return true for existing key")
	}

	_, ok := s.Get("key1")
	if ok {
		t.Fatal("expected key1 to be deleted")
	}

	if s.Delete("key1") {
		t.Fatal("expected Delete to return false for missing key")
	}
}

func TestStore_TTLExpiry(t *testing.T) {
	s := New()

	// Set with a short TTL.
	s.Set("ephemeral", []byte("temp"), 50*time.Millisecond)

	// Should exist immediately.
	val, ok := s.Get("ephemeral")
	if !ok {
		t.Fatal("expected key to exist immediately")
	}
	if string(val) != "temp" {
		t.Fatalf("expected 'temp', got '%s'", string(val))
	}

	// Wait for TTL to expire.
	time.Sleep(100 * time.Millisecond)

	// Lazy expiry: should return miss and clean up.
	_, ok = s.Get("ephemeral")
	if ok {
		t.Fatal("expected key to be expired")
	}
}

func TestStore_ExpireCommand(t *testing.T) {
	s := New()

	s.Set("key1", []byte("value1"), 0) // No expiry.

	// Update TTL.
	if !s.Expire("key1", 50*time.Millisecond) {
		t.Fatal("expected Expire to return true")
	}

	// Should still exist.
	_, ok := s.Get("key1")
	if !ok {
		t.Fatal("expected key to exist before TTL")
	}

	// Wait for expiry.
	time.Sleep(100 * time.Millisecond)

	_, ok = s.Get("key1")
	if ok {
		t.Fatal("expected key to be expired after TTL")
	}
}

func TestStore_ExpireRemoveTTL(t *testing.T) {
	s := New()

	s.Set("key1", []byte("value1"), 50*time.Millisecond)

	// Remove TTL by setting 0 duration.
	if !s.Expire("key1", 0) {
		t.Fatal("expected Expire to return true")
	}

	// Wait past original TTL.
	time.Sleep(100 * time.Millisecond)

	// Should still exist since TTL was removed.
	_, ok := s.Get("key1")
	if !ok {
		t.Fatal("expected key to persist after removing TTL")
	}
}

func TestStore_Keys(t *testing.T) {
	s := New()

	s.Set("a", []byte("1"), 0)
	s.Set("b", []byte("2"), 0)
	s.Set("c", []byte("3"), 0)

	keys := s.Keys()
	if len(keys) != 3 {
		t.Fatalf("expected 3 keys, got %d", len(keys))
	}

	keySet := make(map[string]bool)
	for _, k := range keys {
		keySet[k] = true
	}
	for _, expected := range []string{"a", "b", "c"} {
		if !keySet[expected] {
			t.Fatalf("expected key '%s' in Keys()", expected)
		}
	}
}

func TestStore_Len(t *testing.T) {
	s := New()

	if s.Len() != 0 {
		t.Fatalf("expected 0 length, got %d", s.Len())
	}

	s.Set("a", []byte("1"), 0)
	s.Set("b", []byte("2"), 0)

	if s.Len() != 2 {
		t.Fatalf("expected 2 length, got %d", s.Len())
	}
}

func TestStore_ForEach(t *testing.T) {
	s := New()

	s.Set("a", []byte("1"), 0)
	s.Set("b", []byte("2"), 0)
	s.Set("c", []byte("3"), 0)

	visited := make(map[string]string)
	s.ForEach(func(key string, value []byte, ttl time.Duration) bool {
		visited[key] = string(value)
		return true
	})

	if len(visited) != 3 {
		t.Fatalf("expected 3 entries, visited %d", len(visited))
	}
}

func TestStore_ForEach_StopEarly(t *testing.T) {
	s := New()

	for i := 0; i < 100; i++ {
		s.Set(fmt.Sprintf("key%d", i), []byte("v"), 0)
	}

	count := 0
	s.ForEach(func(key string, value []byte, ttl time.Duration) bool {
		count++
		return count < 5 // Stop after 5.
	})

	if count != 5 {
		t.Fatalf("expected ForEach to stop after 5, visited %d", count)
	}
}

func TestStore_Stats(t *testing.T) {
	s := New()

	s.Set("a", []byte("1"), 0)
	s.Set("b", []byte("2"), 0)
	s.Get("a")
	s.Get("missing")
	s.Delete("b")

	stats := s.Stats()
	if stats.Sets != 2 {
		t.Fatalf("expected 2 sets, got %d", stats.Sets)
	}
	if stats.Hits != 1 {
		t.Fatalf("expected 1 hit, got %d", stats.Hits)
	}
	if stats.Misses != 1 {
		t.Fatalf("expected 1 miss, got %d", stats.Misses)
	}
	if stats.Deletes != 1 {
		t.Fatalf("expected 1 delete, got %d", stats.Deletes)
	}
}

func TestStore_ConcurrentAccess(t *testing.T) {
	s := New()
	var wg sync.WaitGroup

	// 100 concurrent writers.
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				key := fmt.Sprintf("key-%d-%d", id, j)
				s.Set(key, []byte(fmt.Sprintf("val-%d-%d", id, j)), 0)
			}
		}(i)
	}

	// 100 concurrent readers.
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				key := fmt.Sprintf("key-%d-%d", id, j)
				s.Get(key)
			}
		}(i)
	}

	wg.Wait()

	// Verify no panic or data race occurred (run with -race flag).
	if s.Len() == 0 {
		t.Fatal("expected some entries to be present after concurrent writes")
	}
}

func TestSweeper_ExpiresKeys(t *testing.T) {
	s := New()

	// Set 100 keys with 50ms TTL.
	for i := 0; i < 100; i++ {
		s.Set(fmt.Sprintf("key%d", i), []byte("v"), 50*time.Millisecond)
	}

	if s.Len() != 100 {
		t.Fatalf("expected 100 keys, got %d", s.Len())
	}

	// Start sweeper with aggressive interval.
	sw := NewSweeper(s, 10*time.Millisecond)
	sw.Start()

	// Wait for keys to expire and sweeper to clean them.
	time.Sleep(300 * time.Millisecond)

	sw.Stop()

	remaining := s.Len()
	if remaining > 0 {
		t.Fatalf("expected 0 keys after sweep, got %d", remaining)
	}
}

// Benchmarks

func BenchmarkStore_Set(b *testing.B) {
	s := New()
	value := []byte("benchmark-value")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.Set(fmt.Sprintf("key%d", i), value, 0)
	}
}

func BenchmarkStore_Get(b *testing.B) {
	s := New()
	value := []byte("benchmark-value")

	// Pre-populate.
	for i := 0; i < 10000; i++ {
		s.Set(fmt.Sprintf("key%d", i), value, 0)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.Get(fmt.Sprintf("key%d", i%10000))
	}
}

func BenchmarkStore_SetParallel(b *testing.B) {
	s := New()
	value := []byte("benchmark-value")

	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			s.Set(fmt.Sprintf("key%d", i), value, 0)
			i++
		}
	})
}

func BenchmarkStore_GetParallel(b *testing.B) {
	s := New()
	value := []byte("benchmark-value")

	// Pre-populate.
	for i := 0; i < 10000; i++ {
		s.Set(fmt.Sprintf("key%d", i), value, 0)
	}

	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			s.Get(fmt.Sprintf("key%d", i%10000))
			i++
		}
	})
}
