ALTER TABLE chunks ADD COLUMN IF NOT EXISTS faq_index_manifest TEXT NOT NULL DEFAULT '';
CREATE TABLE IF NOT EXISTS faq_index_writes (
    id TEXT PRIMARY KEY, tenant_id BIGINT NOT NULL,
    knowledge_base_id TEXT NOT NULL, knowledge_id TEXT NOT NULL, chunk_id TEXT NOT NULL,
    job_id TEXT NOT NULL DEFAULT '', step_id TEXT NOT NULL DEFAULT '', attempt INTEGER NOT NULL DEFAULT 0,
    base_revision INTEGER NOT NULL CHECK (base_revision >= 0), new_entry BOOLEAN NOT NULL DEFAULT FALSE,
    content_digest TEXT NOT NULL, source_ids JSONB NOT NULL, destination JSONB NOT NULL,
    state TEXT NOT NULL DEFAULT 'pending' CHECK (state IN ('pending', 'indexed', 'deleting', 'deleted')),
    estimated_bytes BIGINT NOT NULL DEFAULT 0 CHECK (estimated_bytes >= 0), storage_released BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_faq_index_scope ON faq_index_writes(tenant_id, knowledge_base_id, updated_at);
CREATE INDEX IF NOT EXISTS idx_faq_index_job ON faq_index_writes(job_id, step_id, attempt);
CREATE INDEX IF NOT EXISTS idx_faq_index_chunk ON faq_index_writes(tenant_id, chunk_id);
