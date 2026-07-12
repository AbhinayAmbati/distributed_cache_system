package chaos

import (
	"context"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestFaultInjector_Failed(t *testing.T) {
	fi := NewFaultInjector()
	fi.SetFailed(true)

	if !fi.IsFailed() {
		t.Fatal("expected failed to be true")
	}

	interceptor := fi.UnaryInterceptor()
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return "success", nil
	}

	info := &grpc.UnaryServerInfo{FullMethod: "/test/Method"}
	_, err := interceptor(context.Background(), nil, info, handler)

	if err == nil {
		t.Fatal("expected error from failed node")
	}

	st, ok := status.FromError(err)
	if !ok || st.Code() != codes.Unavailable {
		t.Fatalf("expected Unavailable status code, got %v", st.Code())
	}
}

func TestFaultInjector_Latency(t *testing.T) {
	fi := NewFaultInjector()
	fi.SetLatency(100 * time.Millisecond)

	interceptor := fi.UnaryInterceptor()
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return "success", nil
	}

	start := time.Now()
	info := &grpc.UnaryServerInfo{FullMethod: "/test/Method"}
	resp, err := interceptor(context.Background(), nil, info, handler)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp != "success" {
		t.Fatalf("unexpected response: %v", resp)
	}

	duration := time.Since(start)
	if duration < 100*time.Millisecond {
		t.Fatalf("expected request to take at least 100ms, took %v", duration)
	}
}

func TestFaultInjector_Partition(t *testing.T) {
	fi := NewFaultInjector()
	fi.PartitionNode("node-2", true)

	if !fi.IsPartitioned("node-2") {
		t.Fatal("expected node-2 to be partitioned")
	}
	if fi.IsPartitioned("node-3") {
		t.Fatal("expected node-3 not to be partitioned")
	}

	fi.Recover()
	if fi.IsPartitioned("node-2") {
		t.Fatal("expected all partitions cleared after recover")
	}
}
