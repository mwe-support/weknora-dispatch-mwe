CREATE TABLE processing_legacy_evidence (
 id VARCHAR(64) PRIMARY KEY, tenant_id BIGINT NOT NULL,
 knowledge_base_id VARCHAR(64) NOT NULL, datasource_id VARCHAR(64) NOT NULL,
 run_id VARCHAR(64) NOT NULL, error_ordinal INTEGER NOT NULL CHECK(error_ordinal>0),
 error_digest VARCHAR(64) NOT NULL, external_id VARCHAR(512) NOT NULL, file_id VARCHAR(256) NOT NULL,
 action VARCHAR(32) NOT NULL CHECK(action IN ('late_completion','manual_confirmed','candidate_adopted','retry_requested','retry_admitted','recovered','policy_skipped')),
 knowledge_id VARCHAR(64), source_revision VARCHAR(256), attempt INTEGER NOT NULL DEFAULT 0,
 snapshot_digest VARCHAR(64) NOT NULL, configuration_revision VARCHAR(64) NOT NULL, artifact_digest VARCHAR(64) NOT NULL,
 evidence_reference VARCHAR(256) NOT NULL, evidence_digest VARCHAR(64) NOT NULL,
 job_id VARCHAR(64), drain_id VARCHAR(64), actor VARCHAR(128) NOT NULL, reason VARCHAR(512) NOT NULL,
 operation_request_id VARCHAR(128) NOT NULL, request_digest VARCHAR(64) NOT NULL,
 created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
 CONSTRAINT uq_legacy_resolution UNIQUE(tenant_id,run_id,error_ordinal,error_digest,action),
 CONSTRAINT uq_legacy_operation UNIQUE(tenant_id,operation_request_id)
);
CREATE INDEX processing_legacy_evidence_job ON processing_legacy_evidence(job_id);
CREATE FUNCTION processing_legacy_evidence_immutable() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN RAISE EXCEPTION 'legacy evidence is append-only'; END;
$$;
CREATE TRIGGER processing_legacy_evidence_immutable BEFORE UPDATE OR DELETE ON processing_legacy_evidence
FOR EACH ROW EXECUTE FUNCTION processing_legacy_evidence_immutable();

CREATE TABLE processing_legacy_drains (
 id VARCHAR(64) PRIMARY KEY, tenant_id BIGINT NOT NULL, knowledge_base_id VARCHAR(64) NOT NULL, datasource_id VARCHAR(64) NOT NULL,
 scope_revision VARCHAR(64) NOT NULL, auth_revision VARCHAR(64) NOT NULL, configuration_revision VARCHAR(64) NOT NULL,
 inventory JSONB NOT NULL, evidence_reference VARCHAR(256) NOT NULL, evidence_digest VARCHAR(64) NOT NULL,
 actor VARCHAR(128) NOT NULL, operation_request_id VARCHAR(128) NOT NULL, request_digest VARCHAR(64) NOT NULL,
 checked_at TIMESTAMPTZ NOT NULL, expires_at TIMESTAMPTZ NOT NULL, created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
 CONSTRAINT uq_legacy_drain_operation UNIQUE(tenant_id,operation_request_id)
);
CREATE TRIGGER processing_legacy_drain_immutable BEFORE UPDATE OR DELETE ON processing_legacy_drains
FOR EACH ROW EXECUTE FUNCTION processing_legacy_evidence_immutable();
