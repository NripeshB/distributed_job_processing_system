package store

import (
	"context"

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
