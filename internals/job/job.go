package job

import (
	"encoding/json"
	"errors"
	"time"
)

var ErrorNotFound = errors.New("job not found")

type Job struct {
	ID        string          `json:"id"`
	Type      string          `json:"type"`
	Payload   json.RawMessage `json:"payload"`
	Status    string          `json:"status"`
	CreatedAt time.Time       `json:"created_at"`
}
