package main

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/AbhinayAmbati/distributed_cache_system/client"
)

const (
	numWorkers   = 8            // Number of concurrent goroutines making requests
	totalOps     = 5000000      // Total number of operations to run
	writeRatio   = 0.2          // 20% writes, 80% reads
	keyRange     = 1000         // Keyspace size (0 to 999) to simulate realistic cache hit rates
	valSizeBytes = 512          // 512 bytes value size
)

func main() {
	nodes := map[string]string{
		"node-1": "127.0.0.1:7001",
		"node-2": "127.0.0.1:7002",
		"node-3": "127.0.0.1:7003",
	}

	fmt.Printf("Starting load test against 3-node cluster with %d workers...\n", numWorkers)

	// Initialize the client with L1 cache enabled to test L1 performance impact
	c, err := client.NewClient(
		nodes,
		client.WithDialTimeout(5*time.Second),
		client.WithRequestTimeout(2*time.Second),
		client.WithL1Cache(500, 10*time.Second), // 500 items capacity, 10s default TTL
	)
	if err != nil {
		panic(err)
	}
	defer c.Close()

	// Generate a dummy payload
	payload := make([]byte, valSizeBytes)
	for i := range payload {
		payload[i] = 'A'
	}

	var (
		wg           sync.WaitGroup
		opsRemaining int64 = totalOps
		writeOps     atomic.Uint64
		readOps      atomic.Uint64
		readHits     atomic.Uint64
		readMisses   atomic.Uint64
		errorsCount  atomic.Uint64

		// For tracking error messages
		errsMu sync.Mutex
		errMap = make(map[string]int)
	)

	startTime := time.Now()

	wg.Add(numWorkers)
	for w := 0; w < numWorkers; w++ {
		go func(workerID int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(time.Now().UnixNano() + int64(workerID)))
			ctx := context.Background()

			for {
				ops := atomic.AddInt64(&opsRemaining, -1)
				if ops < 0 {
					break
				}

				key := fmt.Sprintf("loadkey:%d", rng.Intn(keyRange))

				if rng.Float64() < writeRatio {
					err := c.Set(ctx, key, payload, 30*time.Second)
					if err != nil {
						errorsCount.Add(1)
						errsMu.Lock()
						errMap[err.Error()]++
						errsMu.Unlock()
					} else {
						writeOps.Add(1)
					}
				} else {
					val, found, err := c.Get(ctx, key)
					if err != nil {
						errorsCount.Add(1)
						errsMu.Lock()
						errMap[err.Error()]++
						errsMu.Unlock()
					} else {
						readOps.Add(1)
						if found {
							readHits.Add(1)
							_ = val
						} else {
							readMisses.Add(1)
						}
					}
				}
			}
		}(w)
	}

	wg.Wait()
	duration := time.Since(startTime)

	// Calculate Stats
	total := writeOps.Load() + readOps.Load() + errorsCount.Load()
	qps := float64(total) / duration.Seconds()
	hitRate := 0.0
	if readOps.Load() > 0 {
		hitRate = float64(readHits.Load()) / float64(readOps.Load()) * 100
	}

	fmt.Println("\n================ LOAD TEST RESULTS ================")
	fmt.Printf("Total Operations:      %d\n", total)
	fmt.Printf("Time Taken:            %v\n", duration)
	fmt.Printf("Throughput (QPS):      %.2f req/sec\n", qps)
	fmt.Printf("Successful Writes:     %d\n", writeOps.Load())
	fmt.Printf("Successful Reads:      %d\n", readOps.Load())
	fmt.Printf("  └─ Cache Hits:       %d (%.2f%%)\n", readHits.Load(), hitRate)
	fmt.Printf("  └─ Cache Misses:     %d\n", readMisses.Load())
	fmt.Printf("Errors Encountered:    %d\n", errorsCount.Load())
	
	if len(errMap) > 0 {
		fmt.Println("\n--- Error Breakdown ---")
		for errMsg, count := range errMap {
			fmt.Printf("  - [%d occurrences]: %s\n", count, errMsg)
		}
	}
	fmt.Println("===================================================")
}
