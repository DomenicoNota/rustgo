CREATE TABLE IF NOT EXISTS logs (
    schema_version SMALLINT NOT NULL CONSTRAINT logs_schema_version_check CHECK (schema_version = 1),
    id TEXT PRIMARY KEY,
    service TEXT NOT NULL,
    level TEXT NOT NULL,
    message TEXT NOT NULL,
    attributes JSONB NOT NULL DEFAULT '{}'::jsonb,
    source JSONB NOT NULL DEFAULT '{}'::jsonb,
    timestamp TIMESTAMPTZ NOT NULL,
    received_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
