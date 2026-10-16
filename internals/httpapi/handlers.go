package httpapi

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/NripeshB/distributed_job_processing_system/internals/job"
)

type JobStore interface {
	Create(context.Context, job.Job) (job.Job, error)
	GetByID(context.Context, string) (job.Job, error)
	List(context.Context) ([]job.Job, error)
}

type Handler struct {
	store JobStore
}

func NewHandler(store JobStore) *Handler {
	return &Handler{store: store}
}

func (h *Handler) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("GET /health", h.health)
	mux.HandleFunc("POST /jobs", h.createJob)
	mux.HandleFunc("GET /jobs/{id}", h.getJob)
	mux.HandleFunc("GET /jobs", h.listJobs)
}

type createJobRequest struct {
	Type    string          `json:"type"`
	Payload json.RawMessage `json:"payload"`
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
		writeError(w, http.StatusBadRequest, "invalid JSON request body")
		return
	}

	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		writeError(w, http.StatusBadRequest, "request must contain one JSON object")
		return
	}

	req.Type = strings.TrimSpace(req.Type)
	if req.Type == "" {
		writeError(w, http.StatusBadRequest, "type is required")
		return
	}

	var payloadObject map[string]json.RawMessage
	if err := json.Unmarshal(req.Payload, &payloadObject); err != nil ||
		payloadObject == nil {
		writeError(w, http.StatusBadRequest, "payload must be a JSON object")
		return
	}

	id, err := newID()
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

	created, err := h.store.Create(r.Context(), j)
	if err != nil {
		log.Printf("create job: %v", err)
		writeError(w, http.StatusInternalServerError, "could not create job")
		return
	}

	writeJSON(w, http.StatusCreated, created)
}

func (h *Handler) getJob(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")

	j, err := h.store.GetByID(r.Context(), id)
	if errors.Is(err, job.ErrorNotFound) {
		writeError(w, http.StatusNotFound, "job not found")
		return
	}
	if err != nil {
		log.Printf("get job: %v", err)
		writeError(w, http.StatusInternalServerError, "could not retrieve job")
		return
	}

	writeJSON(w, http.StatusOK, j)
}

func (h *Handler) listJobs(w http.ResponseWriter, r *http.Request) {
	jobs, err := h.store.List(r.Context())
	if err != nil {
		log.Printf("list jobs: %v", err)
		writeError(w, http.StatusInternalServerError, "could not retrieve jobs")
		return
	}

	writeJSON(w, http.StatusOK, jobs)
}

func newID() (string, error) {
	bytes := make([]byte, 16)
	if _, err := rand.Read(bytes); err != nil {
		return "", err
	}

	return hex.EncodeToString(bytes), nil
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)

	if err := json.NewEncoder(w).Encode(value); err != nil {
		log.Printf("write JSON response: %v", err)
	}
}

func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]string{
		"error": message,
	})
}
