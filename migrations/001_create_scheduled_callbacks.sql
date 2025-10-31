-- TODO: Integrate with real migration tooling (e.g., goose) once selected.
CREATE EXTENSION IF NOT EXISTS "pgcrypto";

CREATE TABLE IF NOT EXISTS scheduled_callbacks (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    idempotency_key text UNIQUE,
    callback_url text NOT NULL,
    method text NOT NULL DEFAULT 'POST',
    headers jsonb,
    body bytea,
    scheduled_for timestamptz NOT NULL,
    status text NOT NULL DEFAULT 'PENDING',
    attempts int NOT NULL DEFAULT 0,
    max_attempts int NOT NULL DEFAULT 5,
    retry_policy jsonb NULL,
    created_at timestamptz DEFAULT now(),
    last_attempt_at timestamptz NULL,
    next_attempt_at timestamptz NULL,
    completed_at timestamptz NULL,
    error_reason text NULL
);

CREATE INDEX IF NOT EXISTS idx_scheduled_callbacks_scheduled_for ON scheduled_callbacks (scheduled_for);

CREATE INDEX IF NOT EXISTS idx_scheduled_callbacks_idempotency ON scheduled_callbacks (idempotency_key);

