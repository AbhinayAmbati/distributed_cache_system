package config

import (
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

// Config holds all configuration for a cache node.
type Config struct {
	// NodeID is a unique identifier for this node in the cluster.
	NodeID string `yaml:"node_id"`

	// GRPCAddr is the address the gRPC server listens on (e.g., ":7001").
	GRPCAddr string `yaml:"grpc_addr"`

	// HTTPAddr is the address the HTTP admin/metrics server listens on (e.g., ":9001").
	HTTPAddr string `yaml:"http_addr"`

	// GossipAddr is the address used for SWIM gossip protocol (e.g., ":8001").
	GossipAddr string `yaml:"gossip_addr"`

	// SeedNodes is a list of known node addresses to join the cluster.
	SeedNodes []string `yaml:"seed_nodes"`

	// ReplicaCount is the number of replicas for each key (including primary).
	ReplicaCount int `yaml:"replica_count"`

	// VirtualNodes is the number of virtual nodes per physical node on the hash ring.
	VirtualNodes int `yaml:"virtual_nodes"`

	// SweepInterval is how often the TTL sweeper runs.
	SweepInterval time.Duration `yaml:"sweep_interval"`

	// MaxLoadFactor is ε for consistent hashing with bounded loads.
	// Max load on any node = (1 + ε) × average load.
	MaxLoadFactor float64 `yaml:"max_load_factor"`

	// LogLevel controls the verbosity of logging ("debug", "info", "warn", "error").
	LogLevel string `yaml:"log_level"`
}

// DefaultConfig returns a Config with sensible default values.
func DefaultConfig() *Config {
	return &Config{
		NodeID:        "",
		GRPCAddr:      ":7001",
		HTTPAddr:      ":9001",
		GossipAddr:    ":8001",
		SeedNodes:     nil,
		ReplicaCount:  3,
		VirtualNodes:  150,
		SweepInterval: 100 * time.Millisecond,
		MaxLoadFactor: 0.25,
		LogLevel:      "info",
	}
}

// LoadFromFile reads a YAML configuration file and merges it with defaults.
func LoadFromFile(path string) (*Config, error) {
	cfg := DefaultConfig()

	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return cfg, nil // Use defaults if file doesn't exist
		}
		return nil, fmt.Errorf("reading config file: %w", err)
	}

	if err := yaml.Unmarshal(data, cfg); err != nil {
		return nil, fmt.Errorf("parsing config file: %w", err)
	}

	return cfg, nil
}

// Validate checks that the configuration is valid.
func (c *Config) Validate() error {
	if c.NodeID == "" {
		return fmt.Errorf("node_id is required")
	}
	if c.GRPCAddr == "" {
		return fmt.Errorf("grpc_addr is required")
	}
	if c.HTTPAddr == "" {
		return fmt.Errorf("http_addr is required")
	}
	if c.ReplicaCount < 1 {
		return fmt.Errorf("replica_count must be >= 1, got %d", c.ReplicaCount)
	}
	if c.VirtualNodes < 1 {
		return fmt.Errorf("virtual_nodes must be >= 1, got %d", c.VirtualNodes)
	}
	if c.SweepInterval <= 0 {
		return fmt.Errorf("sweep_interval must be positive, got %v", c.SweepInterval)
	}
	if c.MaxLoadFactor < 0 {
		return fmt.Errorf("max_load_factor must be >= 0, got %f", c.MaxLoadFactor)
	}
	return nil
}
