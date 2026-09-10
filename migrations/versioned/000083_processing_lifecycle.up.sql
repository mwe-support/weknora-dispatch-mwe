CREATE TABLE IF NOT EXISTS processing_storage_reservations (
    id VARCHAR(36) PRIMARY KEY,
    tenant_id BIGINT NOT NULL,
    storage_backend_id VARCHAR(36) NOT NULL DEFAULT '',
    job_id VARCHAR(64) NOT NULL,
    step_id VARCHAR(64) NOT NULL,
    attempt INTEGER NOT NULL,
    bytes BIGINT NOT NULL CHECK (bytes >= 0),
    temporary BOOLEAN NOT NULL DEFAULT FALSE,
    kind VARCHAR(16) NOT NULL DEFAULT 'file' CHECK (kind IN ('file', 'index')),
    state VARCHAR(16) NOT NULL CHECK (state IN ('reserved', 'committed', 'released')),
    resource_id VARCHAR(36) NOT NULL DEFAULT '',
    physical_path TEXT NOT NULL DEFAULT '',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_processing_storage_tenant ON processing_storage_reservations(tenant_id, state);
CREATE INDEX IF NOT EXISTS idx_processing_storage_job ON processing_storage_reservations(job_id);
CREATE UNIQUE INDEX IF NOT EXISTS idx_processing_storage_resource ON processing_storage_reservations(resource_id) WHERE resource_id <> '';

-- Existing lifecycle resources were not included in legacy knowledge charges.
-- Only newly inserted receipts contribute to this one-time reconciliation.
WITH added AS (
    INSERT INTO processing_storage_reservations(id, tenant_id, job_id, step_id, attempt, bytes, temporary, state, resource_id, physical_path, storage_backend_id)
    SELECT id, tenant_id, creation_job_id, '', 0, GREATEST(size, 0), FALSE, 'committed', id, physical_path, storage_backend_id
    FROM resources WHERE creation_job_id <> '' AND state <> 'deleted'
    ON CONFLICT DO NOTHING RETURNING tenant_id, bytes
), charges AS (SELECT tenant_id, SUM(bytes) AS bytes FROM added GROUP BY tenant_id)
UPDATE tenants SET storage_used = storage_used + charges.bytes FROM charges WHERE tenants.id = charges.tenant_id;
