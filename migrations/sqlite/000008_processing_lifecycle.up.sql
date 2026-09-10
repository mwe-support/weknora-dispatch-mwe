ALTER TABLE wiki_pages ADD COLUMN mutation_revision INTEGER NOT NULL DEFAULT 1;
CREATE TABLE IF NOT EXISTS processing_wiki_writes (
    id TEXT PRIMARY KEY, tenant_id INTEGER NOT NULL, job_id TEXT NOT NULL,
    step_id TEXT NOT NULL, step_attempt INTEGER NOT NULL, publication_epoch INTEGER NOT NULL,
    knowledge_base_id TEXT NOT NULL, knowledge_id TEXT NOT NULL, page_id TEXT NOT NULL,
    slug TEXT NOT NULL, state TEXT NOT NULL CHECK (state IN ('active', 'retracted')),
    artifact_ref TEXT NOT NULL, artifact_digest TEXT NOT NULL, input_fingerprint TEXT NOT NULL,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP, updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (step_id, step_attempt, publication_epoch)
);
CREATE INDEX IF NOT EXISTS idx_processing_wiki_job ON processing_wiki_writes(job_id, state);
CREATE INDEX IF NOT EXISTS idx_processing_wiki_page ON processing_wiki_writes(tenant_id, knowledge_base_id, page_id, state);
