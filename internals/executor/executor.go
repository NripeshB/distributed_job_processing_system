package executor

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/NripeshB/distributed_job_processing_system/internals/job"
)

func Execute(ctx context.Context, j job.Job) error {
	switch j.Type {
	case "email":
		return executeEmail(ctx, j.Payload)
	default:
		return fmt.Errorf("unsupported Error type %s", j.Type)
	}
}

func executeEmail(ctx context.Context, Payload json.RawMessage) error {
	var data struct {
		To string `json:"to"`
	}
	if err := json.Unmarshal(Payload, &data); err != nil {
		return fmt.Errorf("decode email payload %w:", err)
	}
	if data.To == "" {
		return fmt.Errorf("email recipient is required")
	}

	// simulate email delivery to To

	log.Printf("Simulating email delivery to %s", data.To)
	timer := time.NewTimer(2 * time.Second)
	defer timer.Stop()

	select {
	case <-timer.C:
		log.Printf("Finished simulated email to %s", data.To)
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}

}
