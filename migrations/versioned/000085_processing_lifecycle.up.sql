ALTER TABLE wiki_pages ADD COLUMN IF NOT EXISTS mutation_revision BIGINT NOT NULL DEFAULT 1;
CREATE TABLE IF NOT EXISTS processing_wiki_writes (
    id VARCHAR(36) PRIMARY KEY,
    tenant_id BIGINT NOT NULL,
    job_id VARCHAR(36) NOT NULL,
    step_id VARCHAR(36) NOT NULL,
    step_attempt INTEGER NOT NULL,
    publication_epoch BIGINT NOT NULL,
    knowledge_base_id VARCHAR(36) NOT NULL,
    knowledge_id VARCHAR(36) NOT NULL,
    page_id VARCHAR(36) NOT NULL,
    slug VARCHAR(255) NOT NULL,
    state VARCHAR(16) NOT NULL CHECK (state IN ('active', 'retracted')),
    artifact_ref TEXT NOT NULL,
    artifact_digest VARCHAR(64) NOT NULL,
    input_fingerprint VARCHAR(64) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (step_id, step_attempt, publication_epoch)
);
CREATE INDEX IF NOT EXISTS idx_processing_wiki_job ON processing_wiki_writes(job_id, state);
CREATE INDEX IF NOT EXISTS idx_processing_wiki_page ON processing_wiki_writes(tenant_id, knowledge_base_id, page_id, state);
