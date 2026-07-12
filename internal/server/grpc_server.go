package server

import (
	"context"
	"fmt"
	"log"
	"net"
	"time"

	pb "github.com/AbhinayAmbati/distributed_cache_system/api/proto"
	"github.com/AbhinayAmbati/distributed_cache_system/internal/chaos"
	"github.com/AbhinayAmbati/distributed_cache_system/internal/cluster"
	"github.com/AbhinayAmbati/distributed_cache_system/internal/hashing"
	"github.com/AbhinayAmbati/distributed_cache_system/internal/hotkey"
	"github.com/AbhinayAmbati/distributed_cache_system/internal/store"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

// CacheServer implements the gRPC CacheService.
type CacheServer struct {
	nodeID          string
	store           *store.Store
	ring            *hashing.Ring
	replicationMgr  *cluster.ReplicationManager
	faultInjector   *chaos.FaultInjector
	hotKeyMitigator *hotkey.Mitigator
	startTime       time.Time
	grpcSrv         *grpc.Server
}

// NewCacheServer creates a new gRPC cache server.
func NewCacheServer(
	nodeID string,
	s *store.Store,
	ring *hashing.Ring,
	repMgr *cluster.ReplicationManager,
	fi *chaos.FaultInjector,
	mitigator *hotkey.Mitigator,
) *CacheServer {
	return &CacheServer{
		nodeID:          nodeID,
		store:           s,
		ring:            ring,
		replicationMgr:  repMgr,
		faultInjector:   fi,
		hotKeyMitigator: mitigator,
		startTime:       time.Now(),
	}
}

// Start begins listening for gRPC connections on the given address.
func (cs *CacheServer) Start(addr string) error {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", addr, err)
	}

	var opts []grpc.ServerOption
	if cs.faultInjector != nil {
		opts = append(opts, grpc.ChainUnaryInterceptor(cs.loggingInterceptor, cs.faultInjector.UnaryInterceptor()))
		opts = append(opts, grpc.StreamInterceptor(cs.faultInjector.StreamInterceptor()))
	} else {
		opts = append(opts, grpc.UnaryInterceptor(cs.loggingInterceptor))
	}

	cs.grpcSrv = grpc.NewServer(opts...)

	// Register our service.
	RegisterCacheServiceServer(cs.grpcSrv, cs)

	// Enable reflection for grpcurl/debugging.
	reflection.Register(cs.grpcSrv)

	log.Printf("[grpc] listening on %s", addr)
	return cs.grpcSrv.Serve(lis)
}

// Stop gracefully stops the gRPC server.
func (cs *CacheServer) Stop() {
	if cs.grpcSrv != nil {
		cs.grpcSrv.GracefulStop()
		log.Printf("[grpc] server stopped")
	}
}

// loggingInterceptor logs each gRPC call with timing.
func (cs *CacheServer) loggingInterceptor(
	ctx context.Context,
	req interface{},
	info *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
) (interface{}, error) {
	start := time.Now()
	resp, err := handler(ctx, req)
	duration := time.Since(start)

	if err != nil {
		log.Printf("[grpc] %s | %v | ERROR: %v", info.FullMethod, duration, err)
	} else {
		log.Printf("[grpc] %s | %v | OK", info.FullMethod, duration)
	}

	return resp, err
}

// --- gRPC service method implementations ---

// Get retrieves a value by key.
func (cs *CacheServer) Get(ctx context.Context, req *pb.GetRequest) (*pb.GetResponse, error) {
	if cs.ring != nil {
		cs.ring.IncrementLoad(cs.nodeID)
		defer cs.ring.DecrementLoad(cs.nodeID)
	}

	// 1. If not primary/replica, proxy to primary
	if cs.replicationMgr != nil && !cs.replicationMgr.IsReplica(req.Key) {
		primary, ok := cs.replicationMgr.GetPrimary(req.Key)
		if ok && primary != cs.nodeID {
			conn, exists := cs.replicationMgr.GetPeerConn(primary)
			if exists {
				resp := &pb.GetResponse{}
				err := conn.Invoke(ctx, "/cache.CacheService/Get", req, resp, grpc.CallContentSubtype("json"))
				return resp, err
			}
		}
	}

	value, found := cs.store.Get(req.Key)

	resp := &pb.GetResponse{
		Key:   req.Key,
		Found: found,
	}

	if found {
		resp.Value = value
		// Set IsHot based on hot key detection
		if cs.hotKeyMitigator != nil {
			isHot := cs.hotKeyMitigator.CheckAndMitigate(req.Key)
			resp.IsHot = isHot
		}
	}

	return resp, nil
}

// Set stores a key-value pair with optional TTL.
func (cs *CacheServer) Set(ctx context.Context, req *pb.SetRequest) (*pb.SetResponse, error) {
	if cs.ring != nil {
		cs.ring.IncrementLoad(cs.nodeID)
		defer cs.ring.DecrementLoad(cs.nodeID)
	}

	// 1. If not primary, proxy to primary
	if cs.replicationMgr != nil && !cs.replicationMgr.IsPrimary(req.Key) {
		primary, ok := cs.replicationMgr.GetPrimary(req.Key)
		if ok && primary != cs.nodeID {
			conn, exists := cs.replicationMgr.GetPeerConn(primary)
			if exists {
				resp := &pb.SetResponse{}
				err := conn.Invoke(ctx, "/cache.CacheService/Set", req, resp, grpc.CallContentSubtype("json"))
				return resp, err
			}
		}
	}

	// 2. Perform write (with key splitting if mitigated)
	var ttl time.Duration
	if req.TtlMs > 0 {
		ttl = time.Duration(req.TtlMs) * time.Millisecond
	}

	if cs.hotKeyMitigator != nil && cs.hotKeyMitigator.IsMitigated(req.Key) {
		cs.hotKeyMitigator.WriteToShards(req.Key, req.Value, ttl)
	} else {
		cs.store.Set(req.Key, req.Value, ttl)
	}

	// 3. Trigger async replication if we are the primary
	if cs.replicationMgr != nil && cs.replicationMgr.IsPrimary(req.Key) {
		cs.replicationMgr.ReplicateWrite(req.Key, req.Value, req.TtlMs)
	}

	return &pb.SetResponse{Success: true}, nil
}

// Delete removes a key from the cache.
func (cs *CacheServer) Delete(ctx context.Context, req *pb.DeleteRequest) (*pb.DeleteResponse, error) {
	if cs.ring != nil {
		cs.ring.IncrementLoad(cs.nodeID)
		defer cs.ring.DecrementLoad(cs.nodeID)
	}

	// 1. If not primary, proxy to primary
	if cs.replicationMgr != nil && !cs.replicationMgr.IsPrimary(req.Key) {
		primary, ok := cs.replicationMgr.GetPrimary(req.Key)
		if ok && primary != cs.nodeID {
			conn, exists := cs.replicationMgr.GetPeerConn(primary)
			if exists {
				resp := &pb.DeleteResponse{}
				err := conn.Invoke(ctx, "/cache.CacheService/Delete", req, resp, grpc.CallContentSubtype("json"))
				return resp, err
			}
		}
	}

	// 2. Perform delete
	existed := cs.store.Delete(req.Key)

	// 3. Trigger async replication delete if we are the primary
	if cs.replicationMgr != nil && cs.replicationMgr.IsPrimary(req.Key) {
		cs.replicationMgr.ReplicateDelete(req.Key)
	}

	return &pb.DeleteResponse{Existed: existed}, nil
}

// Expire updates the TTL on an existing key.
func (cs *CacheServer) Expire(ctx context.Context, req *pb.ExpireRequest) (*pb.ExpireResponse, error) {
	if cs.ring != nil {
		cs.ring.IncrementLoad(cs.nodeID)
		defer cs.ring.DecrementLoad(cs.nodeID)
	}

	// 1. If not primary, proxy to primary
	if cs.replicationMgr != nil && !cs.replicationMgr.IsPrimary(req.Key) {
		primary, ok := cs.replicationMgr.GetPrimary(req.Key)
		if ok && primary != cs.nodeID {
			conn, exists := cs.replicationMgr.GetPeerConn(primary)
			if exists {
				resp := &pb.ExpireResponse{}
				err := conn.Invoke(ctx, "/cache.CacheService/Expire", req, resp, grpc.CallContentSubtype("json"))
				return resp, err
			}
		}
	}

	var ttl time.Duration
	if req.TtlMs > 0 {
		ttl = time.Duration(req.TtlMs) * time.Millisecond
	}

	found := cs.store.Expire(req.Key, ttl)

	// 2. Propagate expiration change if found
	if found && cs.replicationMgr != nil && cs.replicationMgr.IsPrimary(req.Key) {
		val, ok := cs.store.Get(req.Key)
		if ok {
			cs.replicationMgr.ReplicateWrite(req.Key, val, req.TtlMs)
		}
	}

	return &pb.ExpireResponse{Found: found}, nil
}

// Keys returns all keys (debug endpoint).
func (cs *CacheServer) Keys(ctx context.Context, req *pb.KeysRequest) (*pb.KeysResponse, error) {
	keys := cs.store.Keys()
	return &pb.KeysResponse{Keys: keys}, nil
}

// Ping checks if the node is alive and returns basic stats.
func (cs *CacheServer) Ping(ctx context.Context, req *pb.PingRequest) (*pb.PingResponse, error) {
	uptime := time.Since(cs.startTime)
	return &pb.PingResponse{
		NodeId:    cs.nodeID,
		UptimeMs:  uptime.Milliseconds(),
		KeysCount: int64(cs.store.Len()),
	}, nil
}

// Replicate handles replicated writes from the primary node (Phase 3).
func (cs *CacheServer) Replicate(ctx context.Context, req *pb.ReplicateRequest) (*pb.ReplicateResponse, error) {
	switch req.Op {
	case pb.OpSet:
		var ttl time.Duration
		if req.TtlMs > 0 {
			ttl = time.Duration(req.TtlMs) * time.Millisecond
		}
		cs.store.Set(req.Key, req.Value, ttl)
	case pb.OpDelete:
		cs.store.Delete(req.Key)
	}
	return &pb.ReplicateResponse{Success: true}, nil
}

// TransferKeys streams key-value pairs for bulk transfer (Phase 3).
func (cs *CacheServer) TransferKeys(req *pb.TransferKeysRequest, stream CacheService_TransferKeysServer) error {
	cs.store.ForEach(func(key string, value []byte, ttl time.Duration) bool {
		kv := &pb.KeyValuePair{
			Key:   key,
			Value: value,
			TtlMs: ttl.Milliseconds(),
		}
		if err := stream.Send(kv); err != nil {
			log.Printf("[grpc] TransferKeys stream error: %v", err)
			return false
		}
		return true
	})
	return nil
}
