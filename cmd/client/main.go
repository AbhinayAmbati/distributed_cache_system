package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/AbhinayAmbati/distributed_cache_system/client"
)

func main() {
	// 1. Map cluster nodes to their gRPC addresses.
	nodes := map[string]string{
		"node-1": "127.0.0.1:7001",
		"node-2": "127.0.0.1:7002",
		"node-3": "127.0.0.1:7003",
	}

	fmt.Println("Connecting to the cache cluster...")
	c, err := client.NewClient(
		nodes,
		client.WithDialTimeout(5*time.Second),
		client.WithRequestTimeout(3*time.Second),
		client.WithL1Cache(1000, 5*time.Second), // 1000 items capacity, 5s default TTL
	)
	if err != nil {
		log.Fatalf("Failed to start cache client: %v", err)
	}
	defer c.Close()

	ctx := context.Background()

	// 2. Set some test keys in the cluster.
	testData := map[string]string{
		"user:profile:alice": "Alice Profile Data",
		"user:profile:bob":   "Bob Profile Data",
		"user:profile:charlie": "Charlie Profile Data",
		"system:status":      "All systems nominal",
	}

	fmt.Println("\n--- Setting Keys ---")
	for key, value := range testData {
		err := c.Set(ctx, key, []byte(value), 30*time.Second)
		if err != nil {
			log.Fatalf("Set failed for key %s: %v", key, err)
		}
		fmt.Printf("Successfully set key %q = %q\n", key, value)
	}

	// 3. Read the keys back from the cluster.
	fmt.Println("\n--- Getting Keys ---")
	for key := range testData {
		val, found, err := c.Get(ctx, key)
		if err != nil {
			log.Fatalf("Get failed for key %s: %v", key, err)
		}
		if found {
			fmt.Printf("Retrieved %q = %q\n", key, string(val))
		} else {
			fmt.Printf("Key %q not found!\n", key)
		}
	}

	fmt.Println("\n--- Completed Test Client Execution ---")
}
