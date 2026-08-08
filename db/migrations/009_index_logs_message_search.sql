CREATE INDEX IF NOT EXISTS idx_logs_message_search ON logs USING GIN(to_tsvector('english', message));
