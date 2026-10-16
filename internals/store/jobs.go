package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/NripeshB/distributed_job_processing_system/internals/job"
)

const jobColumns = `id, type , payload, status, created_at`

type scanner interface {
	Scan(dest ...any) error
}

func scanJob(row scanner) (job.Job, error) {
	var j job.Job
	var payload []byte

	err := row.Scan(
		&j.ID,
		&j.Type,
		&payload,
		&j.Status,
		&j.CreatedAt,
	)
	if err != nil {
		return job.Job{}, err
	}

	j.Payload = payload
	return j, nil
}

func (s *PostgresStore) Create(
	ctx context.Context,
	j job.Job,
) (job.Job, error) {
	query := `
	INSERT INTO jobs (id, type, payload, status, created_at)
	VALUES ($1,$2,$3::jsonb,$4,$5)
	RETURNING
	` + jobColumns

	created, err := scanJob(s.db.QueryRowContext(ctx, query, j.ID, j.Type, string(j.Payload), j.Status, j.CreatedAt))
	if err != nil {
		return job.Job{}, err
	}
	return created, nil

}

func (s *PostgresStore) GetByID(
	ctx context.Context,
	id string,
) (job.Job, error) {

	query := `SELECT` + jobColumns + `FROM job WHERE id = $1`
	j, err := scanJob(s.db.QueryRowContext(ctx, query, id))

	if errors.Is(err, sql.ErrNoRows) {
		return job.Job{}, fmt.Errorf("%w, %s", job.ErrorNotFound, id)
	}
	if err != nil {
		return job.Job{}, fmt.Errorf("query job by id: %w", err)
	}
	return j, nil
}

func (s *PostgresStore) List(ctx context.Context) ([]job.Job, error) {

	query := `SELECT` + jobColumns + `FROM job ORDER BY created_at DESC, id ASC`
	rows, err := s.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("query jobs: %w", err)
	}
	defer rows.Close()
	jobs := make([]job.Job, 0)

	for rows.Next() {
		j, err := scanJob(rows)
		if err != nil {
			return nil, fmt.Errorf("scan job: %w", err)
		}

		jobs = append(jobs, j)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate jobs: %w", err)
	}
	return jobs, nil

}

func (s *PostgresStore) UpdateStatus(ctx context.Context, id string, status string) error {
	result, err := s.db.ExecContext(
		ctx,
		`UPDATE jobs SET status = $2 WHERE id = $1`,
		id,
		status,
	)
	if err != nil {
		return fmt.Errorf("update job status: %w", err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("rows affected cout %w", err)
	}
	if affected == 0 {
		return fmt.Errorf("%w: %s", job.ErrorNotFound, id)
	}
	return nil

}
