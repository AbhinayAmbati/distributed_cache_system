package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/AbhinayAmbati/distributed_cache_system/config"
	"github.com/AbhinayAmbati/distributed_cache_system/internal/chaos"
	"github.com/AbhinayAmbati/distributed_cache_system/internal/cluster"
	"github.com/AbhinayAmbati/distributed_cache_system/internal/hashing"
	"github.com/AbhinayAmbati/distributed_cache_system/internal/hotkey"
	"github.com/AbhinayAmbati/distributed_cache_system/internal/server"
	"github.com/AbhinayAmbati/distributed_cache_system/internal/store"
)

var (
	version = "0.1.0"
)

func main() {
	// --- Parse CLI flags ---
	configPath := flag.String("config", "", "Path to YAML config file")
	nodeID := flag.String("node-id", "", "Unique node identifier (overrides config)")
	grpcAddr := flag.String("grpc-addr", "", "gRPC listen address (overrides config)")
	httpAddr := flag.String("http-addr", "", "HTTP admin listen address (overrides config)")
	flag.Parse()

	// --- Load configuration ---
	var cfg *config.Config
	var err error

	if *configPath != "" {
		cfg, err = config.LoadFromFile(*configPath)
		if err != nil {
			log.Fatalf("Failed to load config: %v", err)
		}
	} else {
		cfg = config.DefaultConfig()
	}

	// Apply CLI overrides.
	if *nodeID != "" {
		cfg.NodeID = *nodeID
	}
	if *grpcAddr != "" {
		cfg.GRPCAddr = *grpcAddr
	}
	if *httpAddr != "" {
		cfg.HTTPAddr = *httpAddr
	}

	// Generate a default node ID if not set.
	if cfg.NodeID == "" {
		hostname, _ := os.Hostname()
		cfg.NodeID = fmt.Sprintf("node-%s-%s", hostname, cfg.GRPCAddr)
	}

	if err := cfg.Validate(); err != nil {
		log.Fatalf("Invalid configuration: %v", err)
	}

	log.Printf("Starting distributed cache node [%s]", cfg.NodeID)
	log.Printf("  gRPC: %s | HTTP: %s | Gossip: %s", cfg.GRPCAddr, cfg.HTTPAddr, cfg.GossipAddr)

	// --- Initialize storage engine ---
	kvStore := store.New()
	log.Printf("  Storage engine: sharded in-memory (256 shards)")

	// --- Start TTL sweeper ---
	sweeper := store.NewSweeper(kvStore, cfg.SweepInterval)
	sweeper.Start()

	// --- Initialize Hash Ring, Registry, Replication Manager, and Chaos/Hotkey components ---
	ring := hashing.NewRing(cfg.VirtualNodes, cfg.MaxLoadFactor)
	ring.AddNode(cfg.NodeID)
	registry := cluster.NewNodeRegistry(cfg.NodeID)
	repMgr := cluster.NewReplicationManager(cfg.NodeID, kvStore, ring, registry, cfg.ReplicaCount)
	fi := chaos.NewFaultInjector()

	// Hot key detection (threshold 100 accesses, decayed every 60s)
	detector := hotkey.NewDetector(100, 60*time.Second)
	detector.Start()
	mitigator := hotkey.NewMitigator(kvStore, detector, 4)

	// --- Start Gossip Membership ---
	selfInfo := cluster.NewNodeInfo(cfg.NodeID, cfg.GRPCAddr, cfg.GossipAddr, cfg.HTTPAddr)
	swim := cluster.NewMembership(selfInfo, registry)
	swim.SetFaultInjector(fi)

	// Set ring callbacks
	ring.SetOnNodeAdded(func(nodeID string) {
		log.Printf("[ring] node added to ring: %s", nodeID)
		info, ok := registry.Get(nodeID)
		if ok && nodeID != cfg.NodeID {
			repMgr.ConnectToPeer(nodeID, info.GRPCAddr)
		}
	})

	ring.SetOnNodeRemoved(func(nodeID string) {
		log.Printf("[ring] node removed from ring: %s", nodeID)
		repMgr.HandleNodeFailure(nodeID)
	})

	// Set gossip event callbacks
	swim.SetOnEvent(func(event cluster.MembershipEvent) {
		switch event.Type {
		case cluster.EventJoin, cluster.EventAlive:
			info, ok := registry.Get(event.NodeID)
			if ok {
				ring.AddNode(event.NodeID)
				if event.NodeID != cfg.NodeID {
					repMgr.ConnectToPeer(event.NodeID, info.GRPCAddr)
				}
			}
		case cluster.EventDead, cluster.EventLeft:
			ring.RemoveNode(event.NodeID)
		}
	})

	// Start Gossip server
	if err := swim.Start(cfg.GossipAddr); err != nil {
		log.Fatalf("gossip server error: %v", err)
	}

	// Join existing cluster if seeds are configured
	if len(cfg.SeedNodes) > 0 {
		if err := swim.JoinCluster(cfg.SeedNodes); err != nil {
			log.Printf("[swim] failed to join seed nodes: %v", err)
		}
	}

	// --- Start HTTP admin server ---
	httpSrv := server.NewHTTPServer(cfg.NodeID, kvStore, ring)
	
	// Register chaos testing routes
	chaosHandler := chaos.NewHandler(fi)
	chaosHandler.RegisterRoutes(httpSrv.Mux())

	go func() {
		if err := httpSrv.Start(cfg.HTTPAddr); err != nil {
			log.Printf("HTTP server error: %v", err)
		}
	}()

	// --- Start gRPC server ---
	grpcSrv := server.NewCacheServer(cfg.NodeID, kvStore, ring, repMgr, fi, mitigator)
	go func() {
		if err := grpcSrv.Start(cfg.GRPCAddr); err != nil {
			log.Fatalf("gRPC server error: %v", err)
		}
	}()

	// --- Wait for shutdown signal ---
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	sig := <-sigCh
	log.Printf("Received signal %v, shutting down gracefully...", sig)

	// --- Graceful shutdown ---
	grpcSrv.Stop()
	httpSrv.Stop()
	swim.Stop()
	repMgr.Close()
	detector.Stop()
	sweeper.Stop()

	log.Printf("Node [%s] stopped.", cfg.NodeID)
}
