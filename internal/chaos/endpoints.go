package chaos

import (
	"encoding/json"
	"net/http"
	"time"
)

// Handler handles chaos HTTP request routing and updates the fault injector state.
type Handler struct {
	injector *FaultInjector
}

// NewHandler creates a new chaos HTTP handler.
func NewHandler(fi *FaultInjector) *Handler {
	return &Handler{
		injector: fi,
	}
}

// RegisterRoutes registers the chaos endpoints to the given serve mux.
func (h *Handler) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("/chaos/fail-node", h.handleFailNode)
	mux.HandleFunc("/chaos/slow-node", h.handleSlowNode)
	mux.HandleFunc("/chaos/partition", h.handlePartition)
	mux.HandleFunc("/chaos/recover", h.handleRecover)
	mux.HandleFunc("/chaos/status", h.handleStatus)
}

func (h *Handler) handleFailNode(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		Failed bool `json:"failed"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad request: "+err.Error(), http.StatusBadRequest)
		return
	}

	h.injector.SetFailed(req.Failed)
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"message": "node failure state updated",
		"failed":  req.Failed,
	})
}

func (h *Handler) handleSlowNode(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		LatencyMs int64 `json:"latency_ms"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad request: "+err.Error(), http.StatusBadRequest)
		return
	}

	duration := time.Duration(req.LatencyMs) * time.Millisecond
	h.injector.SetLatency(duration)
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"message":    "node latency updated",
		"latency_ms": req.LatencyMs,
	})
}

func (h *Handler) handlePartition(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		PeerID    string `json:"peer_id"`
		Partition bool   `json:"partition"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad request: "+err.Error(), http.StatusBadRequest)
		return
	}

	if req.PeerID == "" {
		http.Error(w, "bad request: peer_id is required", http.StatusBadRequest)
		return
	}

	h.injector.PartitionNode(req.PeerID, req.Partition)
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"message":   "node partition state updated",
		"peer_id":   req.PeerID,
		"partition": req.Partition,
	})
}

func (h *Handler) handleRecover(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	h.injector.Recover()
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"message": "all faults recovered",
	})
}

func (h *Handler) handleStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	writeJSON(w, http.StatusOK, h.injector.Status())
}

func writeJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}
