package chaos

import (
	"context"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// FaultInjector manages the chaos engineering state for a node.
type FaultInjector struct {
	mu sync.RWMutex

	// Node status overrides
	failed      bool
	latencyMs   time.Duration
	partitioned map[string]bool // peer node ID -> boolean (if true, drop traffic)
}

// NewFaultInjector creates a new fault injector.
func NewFaultInjector() *FaultInjector {
	return &FaultInjector{
		partitioned: make(map[string]bool),
	}
}

// SetFailed marks the node as crashed (fails all incoming RPCs and UDP pings).
func (fi *FaultInjector) SetFailed(failed bool) {
	fi.mu.Lock()
	defer fi.mu.Unlock()
	fi.failed = failed
}

// IsFailed returns true if the node is in failed state.
func (fi *FaultInjector) IsFailed() bool {
	fi.mu.RLock()
	defer fi.mu.RUnlock()
	return fi.failed
}

// SetLatency sets artificial latency for all requests.
func (fi *FaultInjector) SetLatency(d time.Duration) {
	fi.mu.Lock()
	defer fi.mu.Unlock()
	fi.latencyMs = d
}

// GetLatency returns the injected latency duration.
func (fi *FaultInjector) GetLatency() time.Duration {
	fi.mu.RLock()
	defer fi.mu.RUnlock()
	return fi.latencyMs
}

// PartitionNode partition a peer node (blocks traffic to/from it).
func (fi *FaultInjector) PartitionNode(peerID string, partition bool) {
	fi.mu.Lock()
	defer fi.mu.Unlock()
	if partition {
		fi.partitioned[peerID] = true
	} else {
		delete(fi.partitioned, peerID)
	}
}

// IsPartitioned returns true if traffic to/from a peer should be blocked.
func (fi *FaultInjector) IsPartitioned(peerID string) bool {
	fi.mu.RLock()
	defer fi.mu.RUnlock()
	return fi.partitioned[peerID]
}

// Recover clears all faults.
func (fi *FaultInjector) Recover() {
	fi.mu.Lock()
	defer fi.mu.Unlock()
	fi.failed = false
	fi.latencyMs = 0
	fi.partitioned = make(map[string]bool)
}

// Status returns a summary of active faults.
type FaultStatus struct {
	Failed      bool     `json:"failed"`
	LatencyMs   int64    `json:"latency_ms"`
	Partitioned []string `json:"partitioned"`
}

func (fi *FaultInjector) Status() FaultStatus {
	fi.mu.RLock()
	defer fi.mu.RUnlock()

	partitionedList := make([]string, 0, len(fi.partitioned))
	for peer := range fi.partitioned {
		partitionedList = append(partitionedList, peer)
	}

	return FaultStatus{
		Failed:      fi.failed,
		LatencyMs:   fi.latencyMs.Milliseconds(),
		Partitioned: partitionedList,
	}
}

// UnaryInterceptor returns a gRPC unary interceptor that applies the injected faults.
func (fi *FaultInjector) UnaryInterceptor() grpc.UnaryServerInterceptor {
	return func(
		ctx context.Context,
		req interface{},
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (interface{}, error) {
		fi.mu.RLock()
		failed := fi.failed
		latency := fi.latencyMs
		fi.mu.RUnlock()

		// 1. Check for complete node failure
		if failed {
			return nil, status.Error(codes.Unavailable, "node is crashed (chaos engineering)")
		}

		// 2. Inject artificial latency
		if latency > 0 {
			select {
			case <-time.After(latency):
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}

		return handler(ctx, req)
	}
}

// StreamInterceptor returns a gRPC stream interceptor that applies the injected faults.
func (fi *FaultInjector) StreamInterceptor() grpc.StreamServerInterceptor {
	return func(
		srv interface{},
		ss grpc.ServerStream,
		info *grpc.StreamServerInfo,
		handler grpc.StreamHandler,
	) error {
		fi.mu.RLock()
		failed := fi.failed
		latency := fi.latencyMs
		fi.mu.RUnlock()

		if failed {
			return status.Error(codes.Unavailable, "node is crashed (chaos engineering)")
		}

		if latency > 0 {
			select {
			case <-time.After(latency):
			case <-ss.Context().Done():
				return ss.Context().Err()
			}
		}

		return handler(srv, ss)
	}
}
