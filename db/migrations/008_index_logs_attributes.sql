CREATE INDEX IF NOT EXISTS idx_logs_attributes ON logs USING GIN(attributes);
