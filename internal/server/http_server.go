package server

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/AbhinayAmbati/distributed_cache_system/internal/store"
)

// HTTPServer provides admin, health, and metrics endpoints.
type HTTPServer struct {
	nodeID    string
	store     *store.Store
	startTime time.Time
	server    *http.Server
	mux       *http.ServeMux
}

// NewHTTPServer creates a new HTTP admin server.
func NewHTTPServer(nodeID string, s *store.Store) *HTTPServer {
	h := &HTTPServer{
		nodeID:    nodeID,
		store:     s,
		startTime: time.Now(),
		mux:       http.NewServeMux(),
	}

	h.registerRoutes()
	return h
}

// registerRoutes sets up all HTTP endpoints.
func (h *HTTPServer) registerRoutes() {
	h.mux.HandleFunc("/health", h.handleHealth)
	h.mux.HandleFunc("/metrics", h.handleMetrics)
	h.mux.HandleFunc("/info", h.handleInfo)

	// Phase 5: Chaos testing endpoints will be registered here.
}

// Mux returns the underlying serve mux to allow custom route registration.
func (h *HTTPServer) Mux() *http.ServeMux {
	return h.mux
}

// Start begins listening for HTTP connections.
func (h *HTTPServer) Start(addr string) error {
	h.server = &http.Server{
		Addr:         addr,
		Handler:      h.mux,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	log.Printf("[http] admin server listening on %s", addr)
	return h.server.ListenAndServe()
}

// Stop gracefully shuts down the HTTP server.
func (h *HTTPServer) Stop() error {
	if h.server != nil {
		log.Printf("[http] admin server stopping")
		return h.server.Close()
	}
	return nil
}

// --- Handlers ---

// healthResponse is the JSON structure for /health.
type healthResponse struct {
	Status string `json:"status"`
	NodeID string `json:"node_id"`
	Uptime string `json:"uptime"`
}

// handleHealth returns a simple health check response.
func (h *HTTPServer) handleHealth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	resp := healthResponse{
		Status: "ok",
		NodeID: h.nodeID,
		Uptime: time.Since(h.startTime).Round(time.Second).String(),
	}

	writeJSON(w, http.StatusOK, resp)
}

// metricsResponse is the JSON structure for /metrics.
type metricsResponse struct {
	NodeID     string `json:"node_id"`
	KeysCount  int    `json:"keys_count"`
	Hits       uint64 `json:"hits"`
	Misses     uint64 `json:"misses"`
	Sets       uint64 `json:"sets"`
	Deletes    uint64 `json:"deletes"`
	Expirations uint64 `json:"expirations"`
	HitRate    string `json:"hit_rate"`
	UptimeMs   int64  `json:"uptime_ms"`
}

// handleMetrics returns operational metrics.
func (h *HTTPServer) handleMetrics(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	stats := h.store.Stats()

	hitRate := "N/A"
	total := stats.Hits + stats.Misses
	if total > 0 {
		hitRate = fmt.Sprintf("%.2f%%", float64(stats.Hits)/float64(total)*100)
	}

	resp := metricsResponse{
		NodeID:      h.nodeID,
		KeysCount:   h.store.Len(),
		Hits:        stats.Hits,
		Misses:      stats.Misses,
		Sets:        stats.Sets,
		Deletes:     stats.Deletes,
		Expirations: stats.Expirations,
		HitRate:     hitRate,
		UptimeMs:    time.Since(h.startTime).Milliseconds(),
	}

	writeJSON(w, http.StatusOK, resp)
}

// infoResponse is the JSON structure for /info.
type infoResponse struct {
	NodeID       string `json:"node_id"`
	Version      string `json:"version"`
	GoVersion    string `json:"go_version"`
	ShardCount   int    `json:"shard_count"`
	KeysCount    int    `json:"keys_count"`
	UptimeMs     int64  `json:"uptime_ms"`
}

// handleInfo returns node information.
func (h *HTTPServer) handleInfo(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	resp := infoResponse{
		NodeID:     h.nodeID,
		Version:    "0.1.0",
		GoVersion:  "go1.22",
		ShardCount: 256,
		KeysCount:  h.store.Len(),
		UptimeMs:   time.Since(h.startTime).Milliseconds(),
	}

	writeJSON(w, http.StatusOK, resp)
}

// writeJSON writes a JSON response with the given status code.
func writeJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		log.Printf("[http] error encoding JSON response: %v", err)
	}
}
