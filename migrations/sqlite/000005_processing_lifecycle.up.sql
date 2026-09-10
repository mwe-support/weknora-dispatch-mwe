ALTER TABLE processing_jobs ADD COLUMN index_destination TEXT;
ALTER TABLE resources ADD COLUMN creation_job_id VARCHAR(64) NOT NULL DEFAULT '';
CREATE INDEX IF NOT EXISTS idx_resources_creation_job_id ON resources(creation_job_id);
