package main

import (
	"context"
	"fmt"
	"time"

	pb "github.com/AbhinayAmbati/distributed_cache_system/api/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	nodes := map[string]string{
		"node-1": "127.0.0.1:7001",
		"node-2": "127.0.0.1:7002",
		"node-3": "127.0.0.1:7003",
	}

	for name, addr := range nodes {
		fmt.Printf("Dialing %s (%s) via gRPC...\n", name, addr)
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		conn, err := grpc.DialContext(ctx, addr,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithBlock(),
		)
		cancel()

		if err != nil {
			fmt.Printf("  ❌ Failed to dial %s: %v\n", name, err)
			continue
		}
		defer conn.Close()

		ctx, cancel = context.WithTimeout(context.Background(), 2*time.Second)
		resp := &pb.PingResponse{}
		err = conn.Invoke(ctx, "/cache.CacheService/Ping", &pb.PingRequest{}, resp, grpc.CallContentSubtype("json"))
		cancel()

		if err != nil {
			fmt.Printf("  ❌ Ping failed on %s: %v\n", name, err)
		} else {
			fmt.Printf("  ✅ Ping succeeded! Node ID: %s, Uptime: %dms, Keys: %d\n", resp.NodeId, resp.UptimeMs, resp.KeysCount)
		}
	}
}
