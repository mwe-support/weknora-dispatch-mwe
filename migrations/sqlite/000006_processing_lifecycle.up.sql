CREATE TABLE processing_storage_reservations (
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
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX idx_processing_storage_tenant ON processing_storage_reservations(tenant_id, state);
CREATE INDEX idx_processing_storage_job ON processing_storage_reservations(job_id);
CREATE UNIQUE INDEX idx_processing_storage_resource ON processing_storage_reservations(resource_id) WHERE resource_id <> '';
INSERT INTO processing_storage_reservations(id, tenant_id, job_id, step_id, attempt, bytes, temporary, state, resource_id, physical_path, storage_backend_id)
SELECT id, tenant_id, creation_job_id, '', 0, MAX(size, 0), FALSE, 'committed', id, physical_path, storage_backend_id
FROM resources WHERE creation_job_id <> '' AND state <> 'deleted';
UPDATE tenants SET storage_used = storage_used + COALESCE((SELECT SUM(bytes) FROM processing_storage_reservations WHERE tenant_id = tenants.id), 0);
