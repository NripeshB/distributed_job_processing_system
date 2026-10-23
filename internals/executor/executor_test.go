package executor

import (
	"context"
	"strings"
	"testing"

	"github.com/NripeshB/distributed_job_processing_system/internals/job"
)

func TestExecuteUnsupportedType(t *testing.T) {
	j := job.Job{
		Type:    "unsupported_task",
		Payload: []byte(`{}`),
	}

	err := Execute(context.Background(), j)

	if err == nil {
		t.Fatal("expected unsupported job type to fail")
	}

	if !strings.Contains(err.Error(), "unsupported") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestExecuteEmailMissingRecipient(t *testing.T) {
	j := job.Job{
		Type:    "email",
		Payload: []byte(`{}`),
	}

	err := Execute(context.Background(), j)

	if err == nil {
		t.Fatal("expected missing recipient to fail")
	}
}

func TestExecuteEmailInvalidPayload(t *testing.T) {
	j := job.Job{
		Type:    "email",
		Payload: []byte(`{"to":`),
	}

	err := Execute(context.Background(), j)

	if err == nil {
		t.Fatal("expected invalid JSON to fail")
	}
}

func TestExecuteEmailCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	j := job.Job{
		Type:    "email",
		Payload: []byte(`{"to":"test@example.com"}`),
	}

	err := Execute(ctx, j)

	if err == nil {
		t.Fatal("expected cancelled execution to fail")
	}

	if err != context.Canceled {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
}
