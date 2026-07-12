.PHONY: build run test bench clean

# Build the cache node binary.
build:
	go build -o bin/cachenode ./cmd/cachenode

# Run the cache node with default settings.
run: build
	./bin/cachenode -node-id node-1

# Run with custom ports (useful for multi-node testing).
run-node1: build
	./bin/cachenode -node-id node-1 -grpc-addr :7001 -http-addr :9001

run-node2: build
	./bin/cachenode -node-id node-2 -grpc-addr :7002 -http-addr :9002

run-node3: build
	./bin/cachenode -node-id node-3 -grpc-addr :7003 -http-addr :9003

# Run all unit tests with race detection.
test:
	go test -v -race ./...

# Run benchmarks.
bench:
	go test -bench=. -benchmem ./internal/store/...

# Clean build artifacts.
clean:
	rm -rf bin/
	go clean -testcache
