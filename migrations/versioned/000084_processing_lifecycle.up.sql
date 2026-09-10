CREATE TABLE IF NOT EXISTS processing_graph_writes (
    id VARCHAR(36) PRIMARY KEY,
    tenant_id BIGINT NOT NULL,
    job_id VARCHAR(36) NOT NULL,
    step_id VARCHAR(36) NOT NULL,
    step_attempt INTEGER NOT NULL,
    publication_epoch BIGINT NOT NULL,
    knowledge_base_id VARCHAR(36) NOT NULL,
    knowledge_id VARCHAR(36) NOT NULL,
    chunk_id VARCHAR(36) NOT NULL,
    content_revision INTEGER NOT NULL,
    destination_digest VARCHAR(64) NOT NULL,
    output_digest VARCHAR(64) NOT NULL DEFAULT '',
    state VARCHAR(16) NOT NULL CHECK (state IN ('reserved', 'confirmed', 'deleted')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (step_id, step_attempt, publication_epoch)
);
CREATE INDEX IF NOT EXISTS idx_processing_graph_job ON processing_graph_writes(job_id, state);
CREATE INDEX IF NOT EXISTS idx_processing_graph_kb ON processing_graph_writes(tenant_id, knowledge_base_id, state);
