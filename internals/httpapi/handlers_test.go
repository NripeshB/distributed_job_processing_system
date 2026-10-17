package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/NripeshB/distributed_job_processing_system/internals/job"
)

type fakeJobStore struct {
	jobs      map[string]job.Job
	createErr error
	getErr    error
	listErr   error
}

func newFakeJobStore() *fakeJobStore {
	return &fakeJobStore{jobs: make(map[string]job.Job)}
}

func (s *fakeJobStore) Create(_ context.Context, j job.Job) (job.Job, error) {
	if s.createErr != nil {
		return job.Job{}, s.createErr
	}
	s.jobs[j.ID] = j
	return j, nil
}

func (s *fakeJobStore) GetByID(_ context.Context, id string) (job.Job, error) {
	if s.getErr != nil {
		return job.Job{}, s.getErr
	}
	j, ok := s.jobs[id]
	if !ok {
		return job.Job{}, job.ErrorNotFound
	}
	return j, nil
}

func (s *fakeJobStore) List(_ context.Context) ([]job.Job, error) {
	if s.listErr != nil {
		return nil, s.listErr
	}
	jobs := make([]job.Job, 0, len(s.jobs))
	for _, j := range s.jobs {
		jobs = append(jobs, j)
	}
	return jobs, nil
}

func testMux(store *fakeJobStore) *http.ServeMux {
	mux := http.NewServeMux()
	NewHandler(store).RegisterRoutes(mux)
	return mux
}

func TestHealth(t *testing.T) {
	mux := testMux(newFakeJobStore())
	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	rec := httptest.NewRecorder()

	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}

	var got map[string]string
	if err := json.NewDecoder(rec.Body).Decode(&got); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if got["status"] != "ok" {
		t.Fatalf("status field = %q, want %q", got["status"], "ok")
	}
}

func TestCreateJob(t *testing.T) {
	store := newFakeJobStore()
	mux := testMux(store)

	req := httptest.NewRequest(
		http.MethodPost,
		"/jobs",
		strings.NewReader(`{"type":"email","payload":{"to":"test@example.com"}}`),
	)
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusCreated {
		t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusCreated, rec.Body.String())
	}

	var got job.Job
	if err := json.NewDecoder(rec.Body).Decode(&got); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if got.ID == "" {
		t.Fatal("created job has an empty ID")
	}
	if got.Type != "email" {
		t.Fatalf("type = %q, want %q", got.Type, "email")
	}
	if got.Status != "pending" {
		t.Fatalf("status = %q, want %q", got.Status, "pending")
	}
	if len(got.Payload) == 0 {
		t.Fatal("created job has an empty payload")
	}
	if _, ok := store.jobs[got.ID]; !ok {
		t.Fatalf("created job %q was not passed to the store", got.ID)
	}
}

func TestCreateJobRejectsInvalidRequests(t *testing.T) {
	tests := []struct {
		name string
		body string
	}{
		{name: "invalid JSON", body: `{"type":`},
		{name: "missing type", body: `{"payload":{"to":"test@example.com"}}`},
		{name: "empty type", body: `{"type":"  ","payload":{"to":"test@example.com"}}`},
		{name: "payload is not an object", body: `{"type":"email","payload":[]}`},
		{name: "unknown field", body: `{"type":"email","payload":{},"unexpected":true}`},
		{name: "multiple JSON values", body: `{"type":"email","payload":{}} {"type":"email","payload":{}}`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mux := testMux(newFakeJobStore())
			req := httptest.NewRequest(http.MethodPost, "/jobs", strings.NewReader(tt.body))
			rec := httptest.NewRecorder()

			mux.ServeHTTP(rec, req)

			if rec.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusBadRequest, rec.Body.String())
			}
		})
	}
}

func TestGetJob(t *testing.T) {
	store := newFakeJobStore()
	want := job.Job{
		ID:        "job-123",
		Type:      "email",
		Payload:   json.RawMessage(`{"to":"test@example.com"}`),
		Status:    "pending",
		CreatedAt: time.Date(2026, time.October, 17, 0, 0, 0, 0, time.UTC),
	}
	store.jobs[want.ID] = want
	mux := testMux(store)

	req := httptest.NewRequest(http.MethodGet, "/jobs/"+want.ID, nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}

	var got job.Job
	if err := json.NewDecoder(rec.Body).Decode(&got); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if got.ID != want.ID {
		t.Fatalf("ID = %q, want %q", got.ID, want.ID)
	}
	if got.Type != want.Type {
		t.Fatalf("type = %q, want %q", got.Type, want.Type)
	}
}

func TestGetJobNotFound(t *testing.T) {
	mux := testMux(newFakeJobStore())
	req := httptest.NewRequest(http.MethodGet, "/jobs/does-not-exist", nil)
	rec := httptest.NewRecorder()

	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusNotFound, rec.Body.String())
	}
}

func TestListJobs(t *testing.T) {
	store := newFakeJobStore()
	store.jobs["job-1"] = job.Job{
		ID:        "job-1",
		Type:      "email",
		Payload:   json.RawMessage(`{"to":"one@example.com"}`),
		Status:    "pending",
		CreatedAt: time.Date(2026, time.October, 17, 0, 0, 0, 0, time.UTC),
	}
	store.jobs["job-2"] = job.Job{
		ID:        "job-2",
		Type:      "email",
		Payload:   json.RawMessage(`{"to":"two@example.com"}`),
		Status:    "pending",
		CreatedAt: time.Date(2026, time.October, 17, 1, 0, 0, 0, time.UTC),
	}
	mux := testMux(store)

	req := httptest.NewRequest(http.MethodGet, "/jobs", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}

	var got []job.Job
	if err := json.NewDecoder(rec.Body).Decode(&got); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("job count = %d, want 2; body=%s", len(got), rec.Body.String())
	}
}

func TestGetJobStoreError(t *testing.T) {
	store := newFakeJobStore()
	store.getErr = errors.New("database unavailable")
	mux := testMux(store)

	req := httptest.NewRequest(http.MethodGet, "/jobs/job-123", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusInternalServerError, rec.Body.String())
	}
}

func TestListJobsStoreError(t *testing.T) {
	store := newFakeJobStore()
	store.listErr = errors.New("database unavailable")
	mux := testMux(store)

	req := httptest.NewRequest(http.MethodGet, "/jobs", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusInternalServerError, rec.Body.String())
	}
}

func TestCreateJobStoreError(t *testing.T) {
	store := newFakeJobStore()
	store.createErr = errors.New("database unavailable")
	mux := testMux(store)

	req := httptest.NewRequest(
		http.MethodPost,
		"/jobs",
		strings.NewReader(`{"type":"email","payload":{"to":"test@example.com"}}`),
	)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusInternalServerError, rec.Body.String())
	}
}
