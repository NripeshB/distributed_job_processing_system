package httpapi

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/NripeshB/distributed_job_processing_system/internals/job"
)

type createJobRequest struct {
	Type    string          `json:"type"`
	Payload json.RawMessage `json:"payload"`
}

type Handler struct {
	mu  sync.RWMutex
	job map[string]job.Job
}

func NewHandler() *Handler {
	return &Handler{
		job: make(map[string]job.Job),
	}
}

func (h *Handler) RegisterRouter(mux *http.ServeMux) {
	mux.HandleFunc("GET /health", h.health)
	mux.HandleFunc("POST /jobs", h.createJob)
	mux.HandleFunc("GET /jobs/{id}", h.getJob)
	mux.HandleFunc("GET /jobs", h.listJobs)
}
func (h *Handler) health(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{
		"status": "ok",
	})
}
func (h *Handler) createJob(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)

	var req createJobRequest

	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()

	if err := decoder.Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON request")
		return
	}

	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		writeError(w, http.StatusBadRequest, "Request must contain a JSON value")
		return
	}
	req.Type = strings.TrimSpace(req.Type)
	if req.Type == "" {
		writeError(w, http.StatusBadRequest, "type is required")
		return
	}
	payload := strings.TrimSpace(string(req.Payload))
	if len(payload) == 0 || payload[0] != '{' || !json.Valid(req.Payload) {
		writeError(w, http.StatusInternalServerError, "could not create job")
		return
	}

	id, err := NewID()
	if err != nil {
		log.Printf("generate job ID: %v", err)
		writeError(w, http.StatusInternalServerError, "could not create job")
		return
	}

	j := job.Job{
		ID:        id,
		Type:      req.Type,
		Payload:   req.Payload,
		Status:    "pending",
		CreatedAt: time.Now().UTC(),
	}
	h.mu.Lock()
	h.job[j.ID] = j
	h.mu.Unlock()
	writeJSON(w, http.StatusCreated, j)

}
func (h *Handler) getJob(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")

	h.mu.RLock()
	j, ok := h.job[id]
	h.mu.RUnlock()

	if !ok {
		writeError(w, http.StatusNotFound, "job not found")
	}
	writeJSON(w, http.StatusOK, j)
}

func (h *Handler) listJobs(w http.ResponseWriter, r *http.Request) {
	h.mu.RLock()
	jobs := make([]job.Job, 0, len(h.job))
	for _, j := range h.job {
		jobs = append(jobs, j)
	}
	h.mu.RUnlock()

	sort.Slice(jobs, func(i, j int) bool {
		if jobs[i].CreatedAt.Equal(jobs[j].CreatedAt) {
			return jobs[i].ID < jobs[j].ID
		}
		return jobs[i].CreatedAt.Before(jobs[i].CreatedAt)
	})
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)

	if err := json.NewEncoder(w).Encode(jobs); err != nil {
		log.Printf("encode job responses: %v", err)
	}

}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)

	if err := json.NewEncoder(w).Encode(value); err != nil {
		log.Printf("write JSON reponse: %v", err)
	}

}
func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]string{
		"error": message,
	})
}

func NewID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(b[:]), nil
}
