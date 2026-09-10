-- psql variables: app_schema, observer_schema, observer_role. The monitor
-- reads these projections; granting raw tables would expose source secrets.
BEGIN;
CREATE SCHEMA IF NOT EXISTS :"observer_schema";
CREATE OR REPLACE VIEW :"observer_schema".tenants AS
 SELECT id,name FROM :"app_schema".tenants;
CREATE OR REPLACE VIEW :"observer_schema".knowledge_bases AS
 SELECT id,tenant_id,name,deleted_at FROM :"app_schema".knowledge_bases;
CREATE OR REPLACE VIEW :"observer_schema".data_sources AS
 SELECT id,tenant_id,knowledge_base_id,name,type,status,deleted_at FROM :"app_schema".data_sources;
CREATE OR REPLACE VIEW :"observer_schema".knowledges AS
 SELECT id,tenant_id,knowledge_base_id,title,file_name,file_size,parse_status,channel,
 created_at,updated_at,processed_at,deleted_at,'LEGACY_UNVERIFIED'::text AS error_message,
 jsonb_strip_nulls(jsonb_build_object(
 'datasource_id',metadata->>'datasource_id','external_id',metadata->>'external_id',
 'file_id',metadata->>'file_id','datasource_candidate',metadata->>'datasource_candidate',
 'datasource_processing_failed',CASE WHEN COALESCE(metadata->>'datasource_processing_failed','')<>'' THEN 'unverified' END,
 'source_fetch_completed_at',metadata->>'source_fetch_completed_at',
 'space_name',metadata->>'space_name','space_id',metadata->>'space_id','source_path',metadata->>'source_path')) AS metadata
 FROM :"app_schema".knowledges WHERE COALESCE(metadata->>'processing_protocol','')<>'2';
CREATE OR REPLACE VIEW :"observer_schema".knowledge_processing_spans AS
 SELECT knowledge_id,name,status,finished_at,updated_at,
 CASE WHEN error_code ~ '^[A-Z0-9_]{1,80}$' THEN error_code ELSE 'LEGACY_UNVERIFIED' END AS error_code,
 'LEGACY_UNVERIFIED'::text AS error_message FROM :"app_schema".knowledge_processing_spans;
CREATE OR REPLACE VIEW :"observer_schema".sync_logs AS
 SELECT s.id,s.tenant_id,s.data_source_id,s.status,s.started_at,s.finished_at,s.created_at,s.updated_at,
 s.items_total,s.items_created,s.items_updated,s.items_deleted,s.items_skipped,s.items_failed,
 'LEGACY_UNVERIFIED'::text AS error_message,
 jsonb_build_object('errors',COALESCE((SELECT jsonb_agg(jsonb_strip_nulls(jsonb_build_object(
 'title',e->>'title','source_path',e->>'source_path','external_id',e->>'external_id','file_id',e->>'file_id',
 'occurred_at',e->>'occurred_at','space_id',e->>'space_id',
 'stage',CASE WHEN e->>'stage' ~ '^[a-z0-9_]{1,64}$' THEN e->>'stage' END,
 'category',CASE WHEN e->>'category' ~ '^[A-Z0-9_]{1,80}$' THEN e->>'category'
   WHEN e->>'message' ~ 'download Tencent Docs export exceeds [0-9]+ bytes' THEN 'FILE_SIZE_EXCEEDED' END,
 'code',CASE WHEN e->>'code' ~ '^[A-Z0-9_]{1,80}$' THEN e->>'code' END,
 'limit_bytes',COALESCE(e->>'limit_bytes',substring(e->>'message' from 'download Tencent Docs export exceeds ([0-9]+) bytes')),
 'actual_bytes',e->>'actual_bytes','observed_at_least_bytes',e->>'observed_at_least_bytes',
 'message','LEGACY_UNVERIFIED')))
 FROM jsonb_array_elements(CASE WHEN jsonb_typeof(s.result->'errors')='array' THEN s.result->'errors' ELSE '[]'::jsonb END) e),'[]'::jsonb),
 'faq_completed',COALESCE((SELECT jsonb_object_agg(key,value) FROM jsonb_each_text(CASE WHEN jsonb_typeof(s.result->'faq_completed')='object' THEN s.result->'faq_completed' ELSE '{}'::jsonb END)
 WHERE pg_input_is_valid(value,'timestamp with time zone')),'{}'::jsonb)) AS result
 FROM :"app_schema".sync_logs s WHERE COALESCE(s.result->>'protocol','')<>'2';
CREATE OR REPLACE VIEW :"observer_schema".task_dead_letters AS
 SELECT id,tenant_id,task_type,scope,scope_id,related_id,fail_count,failed_at,'LEGACY_UNVERIFIED'::text AS last_error,
 jsonb_build_object('data_source_id',payload->>'data_source_id') AS payload
 FROM :"app_schema".task_dead_letters WHERE COALESCE(payload->>'protocol','')<>'2';
CREATE OR REPLACE VIEW :"observer_schema".task_pending_ops AS
 SELECT id,tenant_id,task_type,enqueued_at FROM :"app_schema".task_pending_ops
 WHERE step_id IS NULL OR delivered_at IS NULL;
-- Apply on every install/upgrade, including an existing secrets file.
REVOKE ALL ON ALL TABLES IN SCHEMA :"app_schema" FROM :"observer_role";
ALTER DEFAULT PRIVILEGES IN SCHEMA :"app_schema" REVOKE SELECT ON TABLES FROM :"observer_role";
GRANT USAGE ON SCHEMA :"app_schema", :"observer_schema" TO :"observer_role";
GRANT SELECT ON ALL TABLES IN SCHEMA :"observer_schema" TO :"observer_role";
GRANT SELECT ON :"app_schema".mwe_processing_current_run_progress,
 :"app_schema".mwe_processing_current_document_lifecycle,
 :"app_schema".mwe_processing_stage_retry_queue,
 :"app_schema".mwe_processing_unresolved_incidents,
 :"app_schema".mwe_processing_attempt_timeline,
 :"app_schema".mwe_processing_lifecycle_inconsistencies TO :"observer_role";
ALTER ROLE :"observer_role" SET search_path = :"observer_schema", :"app_schema";
ALTER ROLE :"observer_role" SET default_transaction_read_only = on;
COMMIT;
