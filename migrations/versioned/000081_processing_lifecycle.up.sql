CREATE TABLE IF NOT EXISTS processing_artifact_references (
    id VARCHAR(36) PRIMARY KEY,
    tenant_id BIGINT NOT NULL,
    producer_job_id VARCHAR(36) NOT NULL,
    producer_step_id VARCHAR(36) NOT NULL,
    consumer_job_id VARCHAR(36) NOT NULL,
    consumer_step_id VARCHAR(36) NOT NULL,
    attempt INTEGER NOT NULL CHECK (attempt > 0),
    digest VARCHAR(64) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_processing_artifact_reference UNIQUE (producer_step_id, consumer_step_id),
    CONSTRAINT ck_processing_artifact_not_self CHECK (producer_job_id <> consumer_job_id)
);
CREATE INDEX IF NOT EXISTS idx_processing_artifact_producer ON processing_artifact_references (producer_job_id);
CREATE INDEX IF NOT EXISTS idx_processing_artifact_consumer ON processing_artifact_references (consumer_job_id);
