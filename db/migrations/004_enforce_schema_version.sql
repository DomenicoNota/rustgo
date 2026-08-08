DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'logs_schema_version_check'
          AND conrelid = 'logs'::regclass
    ) THEN
        ALTER TABLE logs
            ADD CONSTRAINT logs_schema_version_check CHECK (schema_version = 1);
    END IF;
END
$$;
