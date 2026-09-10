CREATE TABLE IF NOT EXISTS processing_artifact_references (
    id TEXT PRIMARY KEY, tenant_id INTEGER NOT NULL,
    producer_job_id TEXT NOT NULL, producer_step_id TEXT NOT NULL,
    consumer_job_id TEXT NOT NULL, consumer_step_id TEXT NOT NULL,
    attempt INTEGER NOT NULL CHECK (attempt > 0), digest TEXT NOT NULL,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_processing_artifact_reference UNIQUE (producer_step_id, consumer_step_id),
    CONSTRAINT ck_processing_artifact_not_self CHECK (producer_job_id <> consumer_job_id)
);
CREATE INDEX IF NOT EXISTS idx_processing_artifact_producer ON processing_artifact_references (producer_job_id);
CREATE INDEX IF NOT EXISTS idx_processing_artifact_consumer ON processing_artifact_references (consumer_job_id);
