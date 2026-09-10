ALTER TABLE processing_jobs ADD COLUMN IF NOT EXISTS index_destination JSONB;
ALTER TABLE resources ADD COLUMN IF NOT EXISTS creation_job_id VARCHAR(64) NOT NULL DEFAULT '';
CREATE INDEX IF NOT EXISTS idx_resources_creation_job_id ON resources(creation_job_id);
