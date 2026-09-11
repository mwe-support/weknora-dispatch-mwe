CREATE TABLE processing_legacy_evidence (
 id TEXT PRIMARY KEY, tenant_id INTEGER NOT NULL,
 knowledge_base_id TEXT NOT NULL, datasource_id TEXT NOT NULL,
 run_id TEXT NOT NULL, error_ordinal INTEGER NOT NULL CHECK(error_ordinal>0),
 error_digest TEXT NOT NULL, external_id TEXT NOT NULL, file_id TEXT NOT NULL,
 action TEXT NOT NULL CHECK(action IN ('late_completion','manual_confirmed','candidate_adopted','retry_requested','retry_admitted','recovered','policy_skipped')),
 knowledge_id TEXT, source_revision TEXT, attempt INTEGER NOT NULL DEFAULT 0,
 snapshot_digest TEXT NOT NULL, configuration_revision TEXT NOT NULL, artifact_digest TEXT NOT NULL,
 evidence_reference TEXT NOT NULL, evidence_digest TEXT NOT NULL,
 job_id TEXT, drain_id TEXT, actor TEXT NOT NULL, reason TEXT NOT NULL,
 operation_request_id TEXT NOT NULL, request_digest TEXT NOT NULL,
 created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
 CONSTRAINT uq_legacy_resolution UNIQUE(tenant_id,run_id,error_ordinal,error_digest,action),
 CONSTRAINT uq_legacy_operation UNIQUE(tenant_id,operation_request_id)
);
CREATE INDEX processing_legacy_evidence_job ON processing_legacy_evidence(job_id);
CREATE TRIGGER processing_legacy_evidence_no_update BEFORE UPDATE ON processing_legacy_evidence
BEGIN SELECT RAISE(ABORT,'legacy evidence is append-only'); END;

CREATE TABLE processing_legacy_drains (
 id TEXT PRIMARY KEY, tenant_id INTEGER NOT NULL, knowledge_base_id TEXT NOT NULL, datasource_id TEXT NOT NULL,
 scope_revision TEXT NOT NULL, auth_revision TEXT NOT NULL, configuration_revision TEXT NOT NULL,
 inventory TEXT NOT NULL, evidence_reference TEXT NOT NULL, evidence_digest TEXT NOT NULL,
 actor TEXT NOT NULL, operation_request_id TEXT NOT NULL, request_digest TEXT NOT NULL,
 checked_at DATETIME NOT NULL, expires_at DATETIME NOT NULL, created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
 CONSTRAINT uq_legacy_drain_operation UNIQUE(tenant_id,operation_request_id)
);
CREATE TRIGGER processing_legacy_drain_no_update BEFORE UPDATE ON processing_legacy_drains
BEGIN SELECT RAISE(ABORT,'legacy drain evidence is append-only'); END;
CREATE TRIGGER processing_legacy_drain_no_delete BEFORE DELETE ON processing_legacy_drains
BEGIN SELECT RAISE(ABORT,'legacy drain evidence is append-only'); END;
CREATE TRIGGER processing_legacy_evidence_no_delete BEFORE DELETE ON processing_legacy_evidence
BEGIN SELECT RAISE(ABORT,'legacy evidence is append-only'); END;
