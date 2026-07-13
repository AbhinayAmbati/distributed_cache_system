package main

import (
	"context"
	"fmt"
	"time"

	"github.com/AbhinayAmbati/distributed_cache_system/client"
)

func main() {
	nodes := map[string]string{
		"node-1": "127.0.0.1:7001",
		"node-2": "127.0.0.1:7002",
		"node-3": "127.0.0.1:7003",
	}

	c, err := client.NewClient(
		nodes,
		client.WithL1Cache(100, 10*time.Second), // Enable L1 cache (capacity 100)
	)
	if err != nil {
		panic(err)
	}
	defer c.Close()

	ctx := context.Background()
	key := "hot-news-headline"
	c.Set(ctx, key, []byte("Breaking News: Cache works!"), 60*time.Second)

	fmt.Println("Reading key 150 times...")
	var totalDuration time.Duration

	for i := 1; i <= 150; i++ {
		start := time.Now()
		_, _, err := c.Get(ctx, key)
		if err != nil {
			panic(err)
		}
		elapsed := time.Since(start)
		totalDuration += elapsed

		if i == 1 {
			fmt.Printf("  - [1st Read] Latency: %v (gRPC to server)\n", elapsed)
		}
		if i == 100 {
			fmt.Printf("  - [100th Read] Latency: %v (gRPC to server, Count-Min Sketch threshold reached)\n", elapsed)
		}
		if i == 101 {
			fmt.Printf("  - [101st Read] Latency: %v (L1 cache intercepted!)\n", elapsed)
		}
		if i == 150 {
			fmt.Printf("  - [150th Read] Latency: %v (L1 cache intercepted!)\n", elapsed)
		}
	}

	fmt.Printf("\nAverage read latency over 150 reads: %v\n", totalDuration/150)
}
