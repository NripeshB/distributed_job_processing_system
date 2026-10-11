package httpapi

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/NripeshB/distributed_job_processing_system/internals/job"
)

func newTestMux() *http.ServeMux {
	mux := http.NewServeMux()

	handler := NewHandler()
	handler.RegisterRouter(mux)

	return mux
}

func TestHealth(t *testing.T) {
	mux := newTestMux()

	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	rec := httptest.NewRecorder()

	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d",
			http.StatusOK, rec.Code)
	}
}

func TestCreateJob(t *testing.T) {
	mux := newTestMux()

	body := `{
		"type": "send_email",
		"payload": {"email": "user@example.com"}
	}`

	req := httptest.NewRequest(
		http.MethodPost,
		"/jobs",
		strings.NewReader(body),
	)
	req.Header.Set("Content-Type", "application/json")

	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusCreated {
		t.Fatalf("expected status %d, got %d: %s",
			http.StatusCreated, rec.Code, rec.Body.String())
	}

	var created job.Job
	if err := json.NewDecoder(rec.Body).Decode(&created); err != nil {
		t.Fatalf("decode response: %v", err)
	}

	if created.ID == "" {
		t.Fatal("expected a generated ID")
	}

	if created.Type != "send_email" {
		t.Errorf("expected type send_email, got %q", created.Type)
	}

	if created.Status != "pending" {
		t.Errorf("expected status pending, got %q", created.Status)
	}
}

func TestCreateJobRejectsInvalidJSON(t *testing.T) {
	mux := newTestMux()

	req := httptest.NewRequest(
		http.MethodPost,
		"/jobs",
		strings.NewReader(`{"type":`),
	)

	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected status %d, got %d",
			http.StatusBadRequest, rec.Code)
	}
}

func TestCreateJobRejectsMissingType(t *testing.T) {
	mux := newTestMux()

	body := `{"payload":{"email":"user@example.com"}}`
	req := httptest.NewRequest(
		http.MethodPost,
		"/jobs",
		strings.NewReader(body),
	)

	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected status %d, got %d",
			http.StatusBadRequest, rec.Code)
	}
}

func TestGetJobNotFound(t *testing.T) {
	mux := newTestMux()

	req := httptest.NewRequest(
		http.MethodGet,
		"/jobs/unknown",
		nil,
	)

	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected status %d, got %d",
			http.StatusNotFound, rec.Code)
	}
}

func TestCreateThenGetJob(t *testing.T) {
	mux := newTestMux()

	body := `{"type":"send_email","payload":{"email":"a@example.com"}}`
	createReq := httptest.NewRequest(
		http.MethodPost,
		"/jobs",
		strings.NewReader(body),
	)
	createRec := httptest.NewRecorder()

	mux.ServeHTTP(createRec, createReq)

	if createRec.Code != http.StatusCreated {
		t.Fatalf("create: expected %d, got %d",
			http.StatusCreated, createRec.Code)
	}

	var created job.Job
	if err := json.NewDecoder(createRec.Body).Decode(&created); err != nil {
		t.Fatalf("decode created job: %v", err)
	}

	getReq := httptest.NewRequest(
		http.MethodGet,
		"/jobs/"+created.ID,
		nil,
	)
	getRec := httptest.NewRecorder()

	mux.ServeHTTP(getRec, getReq)

	if getRec.Code != http.StatusOK {
		t.Fatalf("get: expected %d, got %d",
			http.StatusOK, getRec.Code)
	}

	var fetched job.Job
	if err := json.NewDecoder(getRec.Body).Decode(&fetched); err != nil {
		t.Fatalf("decode fetched job: %v", err)
	}

	if fetched.ID != created.ID {
		t.Errorf("expected ID %q, got %q", created.ID, fetched.ID)
	}
}

func TestListJobs(t *testing.T) {
	mux := newTestMux()

	// An empty collection should be [] rather than null.
	req := httptest.NewRequest(http.MethodGet, "/jobs", nil)
	rec := httptest.NewRecorder()

	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d",
			http.StatusOK, rec.Code)
	}

	var jobs []job.Job
	if err := json.NewDecoder(rec.Body).Decode(&jobs); err != nil {
		t.Fatalf("decode jobs: %v", err)
	}

	if jobs == nil {
		t.Fatal("expected an empty JSON array, got null")
	}

	if len(jobs) != 0 {
		t.Fatalf("expected 0 jobs, got %d", len(jobs))
	}
}
