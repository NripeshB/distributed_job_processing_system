package store

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/NripeshB/distributed_job_processing_system/internals/job"
)

func openTestStore(t *testing.T) *PostgresStore {
	t.Helper()

	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("TEST_DATABASE_URL is not set; skipping PostgreSQL integration tests")
	}

	u, err := url.Parse(dsn)
	if err != nil {
		t.Fatalf("invalid TEST_DATABASE_URL: %v", err)
	}
	dbName := strings.Trim(u.Path, "/")
	if !strings.HasSuffix(dbName, "_test") {
		t.Fatalf("refusing to run against database %q: name must end in _test", dbName)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	s, err := Open(ctx, dsn)
	if err != nil {
		t.Fatalf("open test database: %v", err)
	}

	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Errorf("close test database: %v", err)
		}
	})

	var pending int
	err = s.db.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM jobs WHERE status = 'pending'",
	).Scan(&pending)
	if err != nil {
		t.Fatalf("check test database schema: %v", err)
	}
	if pending != 0 {
		t.Fatalf("test database contains %d pending job(s); use an empty, isolated test queue", pending)
	}

	return s
}

func insertTestJob(t *testing.T, s *PostgresStore) string {
	t.Helper()

	randomBytes := make([]byte, 16)
	if _, err := rand.Read(randomBytes); err != nil {
		t.Fatalf("generate job ID: %v", err)
	}
	id := hex.EncodeToString(randomBytes)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := s.db.ExecContext(ctx,
		`INSERT INTO jobs (id, type, payload)
		 VALUES ($1, $2, $3::jsonb)`,
		id, "email", `{"to":"integration@example.com"}`,
	)
	if err != nil {
		t.Fatalf("insert test job: %v", err)
	}

	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(
			context.Background(), 5*time.Second,
		)
		defer cleanupCancel()

		_, err := s.db.ExecContext(cleanupCtx,
			"DELETE FROM jobs WHERE id = $1", id,
		)
		if err != nil {
			t.Errorf("delete test job %s: %v", id, err)
		}
	})

	return id
}

type databaseJobState struct {
	status      string
	attempts    int
	startedAt   sql.NullTime
	completedAt sql.NullTime
}

func readJobState(t *testing.T, s *PostgresStore, id string) databaseJobState {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var state databaseJobState
	err := s.db.QueryRowContext(ctx,
		`SELECT status, attempts, started_at, completed_at
		 FROM jobs WHERE id = $1`,
		id,
	).Scan(
		&state.status,
		&state.attempts,
		&state.startedAt,
		&state.completedAt,
	)
	if err != nil {
		t.Fatalf("read job state: %v", err)
	}

	return state
}

func TestClaimNextIntegration(t *testing.T) {
	s := openTestStore(t)
	id := insertTestJob(t, s)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	claimed, err := s.ClaimNext(ctx)
	if err != nil {
		t.Fatalf("ClaimNext failed: %v", err)
	}

	if claimed.ID != id {
		t.Fatalf("claimed ID = %q; want %q", claimed.ID, id)
	}
	if claimed.Status != "processing" {
		t.Errorf("claimed status = %q; want processing", claimed.Status)
	}
	if claimed.Attempts != 1 {
		t.Errorf("claimed attempts = %d; want 1", claimed.Attempts)
	}
	if claimed.StartedAt == nil {
		t.Error("claimed job has nil started_at")
	}

	state := readJobState(t, s, id)
	if state.status != "processing" {
		t.Errorf("database status = %q; want processing", state.status)
	}
	if state.attempts != 1 {
		t.Errorf("database attempts = %d; want 1", state.attempts)
	}
	if !state.startedAt.Valid {
		t.Error("database started_at is NULL")
	}
	if state.completedAt.Valid {
		t.Error("database completed_at should still be NULL")
	}
}

func TestClaimNextEmptyQueueIntegration(t *testing.T) {
	s := openTestStore(t)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_, err := s.ClaimNext(ctx)
	if !errors.Is(err, job.ErrorNotFound) {
		t.Fatalf("ClaimNext error = %v; want job.ErrorNotFound", err)
	}
}

func TestCompleteIntegration(t *testing.T) {
	for _, tc := range []struct {
		name       string
		succeeded  bool
		wantStatus string
	}{
		{"success", true, "succeeded"},
		{"failure", false, "failed"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := openTestStore(t)
			id := insertTestJob(t, s)

			ctx, cancel := context.WithTimeout(
				context.Background(), 10*time.Second,
			)
			defer cancel()

			claimed, err := s.ClaimNext(ctx)
			if err != nil {
				t.Fatalf("claim job: %v", err)
			}
			if claimed.ID != id {
				t.Fatalf("claimed wrong job: %s", claimed.ID)
			}

			if err := s.Complete(ctx, id, tc.succeeded); err != nil {
				t.Fatalf("complete job: %v", err)
			}

			state := readJobState(t, s, id)
			if state.status != tc.wantStatus {
				t.Errorf("status = %q; want %q",
					state.status, tc.wantStatus)
			}
			if state.attempts != 1 {
				t.Errorf("attempts = %d; want 1", state.attempts)
			}
			if !state.startedAt.Valid {
				t.Error("started_at should be populated")
			}
			if !state.completedAt.Valid {
				t.Error("completed_at should be populated")
			}
			if state.startedAt.Valid && state.completedAt.Valid &&
				state.completedAt.Time.Before(state.startedAt.Time) {
				t.Error("completed_at is earlier than started_at")
			}
		})
	}
}

func TestCompletePendingJobIntegration(t *testing.T) {
	s := openTestStore(t)
	id := insertTestJob(t, s)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err := s.Complete(ctx, id, true)
	if err == nil {
		t.Fatal("expected completing a pending job to fail")
	}

	state := readJobState(t, s, id)
	if state.status != "pending" {
		t.Errorf("status = %q; want pending", state.status)
	}
	if state.attempts != 0 {
		t.Errorf("attempts = %d; want 0", state.attempts)
	}
	if state.completedAt.Valid {
		t.Error("completed_at should still be NULL")
	}
}

func TestConcurrentClaimIntegration(t *testing.T) {
	s := openTestStore(t)
	id := insertTestJob(t, s)

	const workers = 2

	type claimResult struct {
		claimedID string
		err       error
	}

	start := make(chan struct{})
	results := make(chan claimResult, workers)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	for i := 0; i < workers; i++ {
		go func() {
			<-start

			claimed, err := s.ClaimNext(ctx)
			results <- claimResult{
				claimedID: claimed.ID,
				err:       err,
			}
		}()
	}

	close(start)

	var successfulClaims int
	var emptyQueueResults int

	for i := 0; i < workers; i++ {
		select {
		case result := <-results:
			switch {
			case result.err == nil:
				successfulClaims++
				if result.claimedID != id {
					t.Errorf("claimed unexpected job %q; want %q",
						result.claimedID, id)
				}
			case errors.Is(result.err, job.ErrorNotFound):
				emptyQueueResults++
			default:
				t.Errorf("unexpected claim error: %v", result.err)
			}
		case <-ctx.Done():
			t.Fatalf("timed out waiting for claim results: %v", ctx.Err())
		}
	}

	if successfulClaims != 1 {
		t.Errorf("successful claims = %d; want exactly 1", successfulClaims)
	}
	if emptyQueueResults != 1 {
		t.Errorf("empty queue results = %d; want exactly 1",
			emptyQueueResults)
	}

	state := readJobState(t, s, id)
	if state.status != "processing" {
		t.Errorf("status = %q; want processing", state.status)
	}
	if state.attempts != 1 {
		t.Errorf("attempts = %d; want 1", state.attempts)
	}
	if !state.startedAt.Valid {
		t.Error("started_at should be populated")
	}

	t.Log(fmt.Sprintf(
		"2 concurrent claimants: %d successful, %d empty-queue; attempts=%d",
		successfulClaims, emptyQueueResults, state.attempts,
	))
}
