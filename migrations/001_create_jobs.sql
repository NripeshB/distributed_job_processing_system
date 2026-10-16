CREATE TABLE jobs (
    id           TEXT PRIMARY KEY,
    type         TEXT NOT NULL CHECK (btrim(type) <> ''),
    payload      JSONB NOT NULL CHECK (jsonb_typeof(payload) = 'object'),
    status       TEXT NOT NULL DEFAULT 'pending'
                 CHECK (status IN ('pending', 'processing', 'succeeded', 'failed')),
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    started_at   TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    attempts     INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0)
);

CREATE INDEX jobs_created_at_id_idx
    ON jobs (created_at, id);
