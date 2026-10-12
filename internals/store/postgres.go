package store

import (
	"context"
	"database/sql"
	"fmt"
	_ "github.com/jackc/pgx/v5/stdlib"
	"time"
)

type PostgresStore struct {
	db *sql.DB
}

func Open(ctx context.Context, dsn string) (*PostgresStore, error) {
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return nil, fmt.Errorf("Opened postgres database :%w", err)
	}
	ok := false
	defer func() {
		if !ok {
			_ = db.Close()
		}
	}()

	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(30 * time.Minute)
	db.SetConnMaxIdleTime(5 * time.Minute)

	pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	if err := db.PingContext(pingCtx); err != nil {
		return nil, fmt.Errorf("ping PostgreSQL: %w", err)
	}
	ok = true
	return &PostgresStore{db: db}, nil

}

func (s *PostgresStore) Close() error {
	return s.db.Close()
}
