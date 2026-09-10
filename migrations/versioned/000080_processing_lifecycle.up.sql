-- Processing protocol 2: execution facts and transactional deliveries.

-- Additive only. Existing documents and Wiki operations are not reclassified.

CREATE TABLE processing_jobs (
    id VARCHAR(64) NOT NULL,
    kind VARCHAR(16) NOT NULL,
    tenant_id BIGINT NOT NULL,
    knowledge_base_id VARCHAR(64) NOT NULL,
    datasource_id VARCHAR(64) NOT NULL,
    external_id VARCHAR(512) NOT NULL,
    origin_run_id VARCHAR(64),
    generation BIGINT NOT NULL,
    scope_revision VARCHAR(64) NOT NULL,
    auth_revision VARCHAR(64),
    source_revision VARCHAR(256) NOT NULL,
    source_digest VARCHAR(64),
    pipeline_fingerprint VARCHAR(64) NOT NULL,
    configuration_revision VARCHAR(64) NOT NULL,
    knowledge_id VARCHAR(64),
    is_current BOOLEAN NOT NULL DEFAULT false,
    is_published BOOLEAN NOT NULL DEFAULT false,
    publication_epoch BIGINT NOT NULL DEFAULT 0,
    active_index_manifest TEXT,
    rollback_pin BOOLEAN NOT NULL DEFAULT false,
    retirement_state VARCHAR(32) NOT NULL DEFAULT 'retained',
    completeness VARCHAR(32) NOT NULL DEFAULT 'unknown',
    readiness VARCHAR(32) NOT NULL DEFAULT 'pending',
    status VARCHAR(32) NOT NULL DEFAULT 'planned',
    revision BIGINT NOT NULL DEFAULT 0,
    plan_sealed BOOLEAN NOT NULL DEFAULT false,
    plan_digest VARCHAR(64),
    metadata JSONB,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    published_at TIMESTAMPTZ,
    finished_at TIMESTAMPTZ,
    PRIMARY KEY (id),
    CHECK (status IN ('planned','enqueue_pending','queued','running','waiting_external','retry_wait','succeeded','blocked','failed','canceled','superseded','skipped')),
    CHECK (kind IN ('document','scan')),
    CHECK (generation > 0 AND revision >= 0 AND publication_epoch >= 0)
);

CREATE TABLE processing_steps (
    id VARCHAR(64) NOT NULL,
    job_id VARCHAR(64) NOT NULL,
    parent_step_id VARCHAR(64),
    stage VARCHAR(64) NOT NULL,
    unit_key VARCHAR(512) NOT NULL,
    kind VARCHAR(16) NOT NULL DEFAULT 'work',
    phase VARCHAR(32) NOT NULL,
    dependencies JSONB,
    input JSONB,
    status VARCHAR(32) NOT NULL DEFAULT 'planned',
    step_attempt INTEGER NOT NULL DEFAULT 1,
    dispatch_seq BIGINT NOT NULL DEFAULT 0,
    lease_token VARCHAR(64),
    lease_expires_at TIMESTAMPTZ,
    heartbeat_at TIMESTAMPTZ,
    progress_at TIMESTAMPTZ,
    input_fingerprint VARCHAR(64) NOT NULL,
    expected_publication_epoch BIGINT NOT NULL DEFAULT 0,
    checkpoint_ref TEXT,
    output_manifest_ref TEXT,
    output_digest VARCHAR(64),
    plan_sealed BOOLEAN NOT NULL DEFAULT false,
    plan_digest VARCHAR(64),
    expected_units INTEGER NOT NULL DEFAULT 0,
    required_for_ready BOOLEAN NOT NULL DEFAULT false,
    required_for_completion BOOLEAN NOT NULL DEFAULT false,
    retry_count INTEGER NOT NULL DEFAULT 0,
    max_retries INTEGER NOT NULL DEFAULT 4,
    next_run_at TIMESTAMPTZ,
    deadline_at TIMESTAMPTZ,
    error_class VARCHAR(64),
    error_code VARCHAR(64),
    error_message TEXT,
    last_error_event_id BIGINT,
    queue_task_id VARCHAR(256),
    result JSONB,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    queued_at TIMESTAMPTZ,
    started_at TIMESTAMPTZ,
    finished_at TIMESTAMPTZ,
    PRIMARY KEY (id),
    CHECK (status IN ('planned','enqueue_pending','queued','running','waiting_external','retry_wait','succeeded','blocked','failed','canceled','superseded','skipped')),
    CHECK (step_attempt > 0 AND dispatch_seq >= 0 AND retry_count >= 0),
    CHECK (phase IN ('prepare','publish','projection','retire','scan'))
);

CREATE TABLE processing_events (
    id BIGSERIAL PRIMARY KEY,
    tenant_id BIGINT NOT NULL,
    job_id VARCHAR(64) NOT NULL,
    job_revision BIGINT NOT NULL,
    run_id VARCHAR(64),
    step_id VARCHAR(64),
    generation BIGINT NOT NULL,
    step_attempt INTEGER,
    dispatch_seq BIGINT,
    lease_token VARCHAR(64),
    event_type VARCHAR(64) NOT NULL,
    from_state VARCHAR(32),
    to_state VARCHAR(32),
    error_class VARCHAR(64),
    error_code VARCHAR(64),
    message TEXT,
    actor VARCHAR(128),
    action VARCHAR(64),
    operation_request_id VARCHAR(128),
    queue_task_id VARCHAR(256),
    trace_id VARCHAR(128),
    resolves_event_id BIGINT,
    resolution_type VARCHAR(64),
    detail JSONB,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE sync_run_items (
    run_id VARCHAR(64) NOT NULL,
    item_key VARCHAR(512) NOT NULL,
    tenant_id BIGINT NOT NULL,
    kind VARCHAR(16) NOT NULL,
    external_id VARCHAR(512) NOT NULL,
    job_id VARCHAR(64),
    disposition VARCHAR(32) NOT NULL,
    source_revision VARCHAR(256),
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (run_id, item_key)
);

CREATE UNIQUE INDEX uq_processing_generation ON processing_jobs (tenant_id, knowledge_base_id, datasource_id, external_id, kind, generation);

CREATE UNIQUE INDEX uq_processing_current ON processing_jobs (tenant_id, knowledge_base_id, datasource_id, external_id, kind) WHERE is_current;

CREATE UNIQUE INDEX uq_processing_published ON processing_jobs (tenant_id, knowledge_base_id, datasource_id, external_id, kind) WHERE is_published;

CREATE UNIQUE INDEX uq_processing_step ON processing_steps (job_id, stage, unit_key);

CREATE UNIQUE INDEX uq_processing_event_revision ON processing_events (job_id, job_revision);

CREATE UNIQUE INDEX uq_processing_operation ON processing_events (tenant_id, action, operation_request_id) WHERE operation_request_id <> '';

CREATE INDEX idx_processing_job_source ON processing_jobs (tenant_id, datasource_id, status);

CREATE INDEX idx_processing_job_knowledge ON processing_jobs (knowledge_id);

CREATE INDEX idx_processing_step_due ON processing_steps (status, next_run_at);

CREATE INDEX idx_processing_step_lease ON processing_steps (status, lease_expires_at);

CREATE INDEX idx_processing_event_run ON processing_events (run_id, id);

CREATE INDEX idx_processing_event_resolution ON processing_events (resolves_event_id);

CREATE INDEX idx_sync_run_item_job ON sync_run_items (job_id);

ALTER TABLE task_pending_ops ADD COLUMN step_id VARCHAR(64);

ALTER TABLE task_pending_ops ADD COLUMN step_attempt INTEGER;

ALTER TABLE task_pending_ops ADD COLUMN dispatch_seq BIGINT;

ALTER TABLE task_pending_ops ADD COLUMN available_at TIMESTAMPTZ;

ALTER TABLE task_pending_ops ADD COLUMN delivered_at TIMESTAMPTZ;

ALTER TABLE task_pending_ops ADD COLUMN queue_task_id VARCHAR(256);

CREATE UNIQUE INDEX uq_processing_delivery ON task_pending_ops (step_id, step_attempt, dispatch_seq) WHERE step_id IS NOT NULL;

CREATE INDEX idx_processing_delivery_due ON task_pending_ops (task_type, delivered_at, available_at);
