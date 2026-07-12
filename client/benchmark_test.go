package client

import (
	"context"
	"fmt"
	"math/rand"
	"log"
	"net"
	"testing"
	"time"

	"github.com/AbhinayAmbati/distributed_cache_system/internal/chaos"
	"github.com/AbhinayAmbati/distributed_cache_system/internal/cluster"
	"github.com/AbhinayAmbati/distributed_cache_system/internal/hashing"
	"github.com/AbhinayAmbati/distributed_cache_system/internal/hotkey"
	"github.com/AbhinayAmbati/distributed_cache_system/internal/server"
	"github.com/AbhinayAmbati/distributed_cache_system/internal/store"
	"google.golang.org/grpc/encoding"
)

// BenchmarkSystem measures get/set throughput and latency under concurrent load.
func BenchmarkSystem(b *testing.B) {
	// 1. Setup a single node cluster
	kvStore := store.New()
	ring := hashing.NewRing(150, 0.25)
	registry := cluster.NewNodeRegistry("node-1")
	repMgr := cluster.NewReplicationManager("node-1", kvStore, ring, registry, 1)
	fi := chaos.NewFaultInjector()
	detector := hotkey.NewDetector(100, 60*time.Second)
	detector.Start()
	defer detector.Stop()
	mitigator := hotkey.NewMitigator(kvStore, detector, 4)

	grpcSrv := server.NewCacheServer("node-1", kvStore, ring, repMgr, fi, mitigator)
	
	// Find an open port
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		b.Fatalf("failed to listen: %v", err)
	}
	addr := lis.Addr().String()
	lis.Close()

	go func() {
		if err := grpcSrv.Start(addr); err != nil {
			// server stopped
		}
	}()
	defer grpcSrv.Stop()

	// Wait for server to start
	time.Sleep(100 * time.Millisecond)

	// 2. Setup Client
	c, err := NewClient(map[string]string{"node-1": addr})
	if err != nil {
		b.Fatalf("failed to create client: %v", err)
	}
	defer c.Close()

	// Debug print codec registration
	import_codec := "google.golang.org/grpc/encoding"
	_ = import_codec
	log.Printf("JSON Codec in registry: %v", encoding.GetCodec("json"))

	ctx := context.Background()
	value := []byte("bench-value-payload")

	b.Run("Set", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			err := c.Set(ctx, fmt.Sprintf("key-%d", i), value, 0)
			if err != nil {
				b.Fatalf("set failed: %v", err)
			}
		}
	})

	b.Run("Get", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _, err := c.Get(ctx, fmt.Sprintf("key-%d", i%1000))
			if err != nil {
				b.Fatalf("get failed: %v", err)
			}
		}
	})

	b.Run("ConcurrentGetSet", func(b *testing.B) {
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			i := 0
			for pb.Next() {
				key := fmt.Sprintf("concurrent-key-%d", i)
				if i%2 == 0 {
					c.Set(ctx, key, value, 0)
				} else {
					c.Get(ctx, key)
				}
				i++
			}
		})
	})
}

// BenchmarkZipfianWorkload benchmarks the system under Zipfian key distribution
// (simulating hot keys) to measure the effectiveness of hot key mitigation.
func BenchmarkZipfianWorkload(b *testing.B) {
	kvStore := store.New()
	ring := hashing.NewRing(150, 0.25)
	registry := cluster.NewNodeRegistry("node-1")
	repMgr := cluster.NewReplicationManager("node-1", kvStore, ring, registry, 1)
	fi := chaos.NewFaultInjector()
	
	// Lower threshold to trigger mitigation quickly during benchmark
	detector := hotkey.NewDetector(10, 60*time.Second)
	detector.Start()
	defer detector.Stop()
	mitigator := hotkey.NewMitigator(kvStore, detector, 4)

	grpcSrv := server.NewCacheServer("node-1", kvStore, ring, repMgr, fi, mitigator)
	
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		b.Fatalf("failed to listen: %v", err)
	}
	addr := lis.Addr().String()
	lis.Close()

	go func() {
		grpcSrv.Start(addr)
	}()
	defer grpcSrv.Stop()

	time.Sleep(100 * time.Millisecond)

	// Client with L1 cache enabled
	c, err := NewClient(map[string]string{"node-1": addr}, WithL1Cache(500, 5*time.Second))
	if err != nil {
		b.Fatalf("failed to create client: %v", err)
	}
	defer c.Close()

	ctx := context.Background()
	value := []byte("zipfian-value")

	// Set up keys
	for i := 0; i < 100; i++ {
		c.Set(ctx, fmt.Sprintf("zipf-%d", i), value, 0)
	}

	// Create Zipf generator to produce hot keys (s=1.5, v=1.0)
	src := rand.NewSource(99)
	r := rand.New(src)
	zipf := rand.NewZipf(r, 1.5, 1.0, 99)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			keyIdx := zipf.Uint64()
			key := fmt.Sprintf("zipf-%d", keyIdx)
			c.Get(ctx, key)
		}
	})
}
