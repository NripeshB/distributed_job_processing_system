package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/NripeshB/distributed_job_processing_system/internals/executor"
	"github.com/NripeshB/distributed_job_processing_system/internals/job"
	"github.com/NripeshB/distributed_job_processing_system/internals/store"
	"github.com/joho/godotenv"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	ctx, stop := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
	)
	defer stop()

	if err := godotenv.Load(); err != nil {
		log.Println("No .env file found; using existing environment variables")

	}

	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		return errors.New("DATABASE_URL environment is required")
	}

	db, err := store.Open(ctx, dsn)
	if err != nil {
		return fmt.Errorf("Connect to postgresql: %w", err)
	}
	defer db.Close()

	log.Println("Worker started")

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		if ctx.Err() != nil {
			log.Println("Robot Stop")
			return nil
		}
		claimed, err := db.ClaimNext(ctx)

		if errors.Is(err, job.ErrorNotFound) {
			select {
			case <-ticker.C:
				continue
			case <-ctx.Done():
				log.Println("Robot Stop")
				return nil

			}
		}
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			log.Printf("Job claiming failed: %v", err)
			select {
			case <-ticker.C:
				continue
			case <-ctx.Done():
				return nil
			}
		}
		log.Printf("Claimed job %s as %s", claimed.ID, claimed.Type)

		executionCtx, cancel := context.WithTimeout(
			context.Background(),
			30*time.Second,
		)
		executionErr := executor.Execute(executionCtx, claimed)
		cancel()

		completionCtx, completionCancel := context.WithTimeout(
			context.Background(),
			5*time.Second,
		)

		err = db.Complete(
			completionCtx,
			claimed.ID,
			executionErr == nil,
		)
		completionCancel()

		if err != nil {
			return fmt.Errorf("record completion for job %s: %w", claimed.ID, err)
		}

		if executionErr != nil {
			log.Printf("Job %s failed: %v", claimed.ID, executionErr)
		} else {
			log.Printf("Job %s succeeded", claimed.ID)
		}

	}

}
