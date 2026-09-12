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
-- Match the original error before mapping its digest to the redacted row.
-- The observer never needs raw messages, operator notes or evidence locations.
CREATE OR REPLACE VIEW :"observer_schema".processing_legacy_evidence AS
 SELECT p.tenant_id,p.knowledge_base_id,p.datasource_id,p.run_id,p.error_ordinal,p.action,
 encode(sha256(convert_to((safe.result->'errors'->(p.error_ordinal-1))::text,'UTF8')),'hex') AS error_digest
 FROM :"app_schema".processing_legacy_evidence p
 JOIN :"app_schema".sync_logs original ON original.id=p.run_id AND original.tenant_id=p.tenant_id AND original.data_source_id=p.datasource_id
 JOIN :"observer_schema".sync_logs safe ON safe.id=original.id AND safe.tenant_id=original.tenant_id AND safe.data_source_id=original.data_source_id
 WHERE p.error_digest=encode(sha256(convert_to((original.result->'errors'->(p.error_ordinal-1))::text,'UTF8')),'hex');
CREATE OR REPLACE VIEW :"observer_schema".task_dead_letters AS
 SELECT id,tenant_id,task_type,scope,scope_id,related_id,fail_count,failed_at,'LEGACY_UNVERIFIED'::text AS last_error,
 jsonb_build_object('data_source_id',payload->>'data_source_id') AS payload
 FROM :"app_schema".task_dead_letters WHERE COALESCE(payload->>'protocol','')<>'2';
CREATE OR REPLACE VIEW :"observer_schema".task_pending_ops AS
 SELECT id,tenant_id,task_type,enqueued_at FROM :"app_schema".task_pending_ops
 WHERE step_id IS NULL OR delivered_at IS NULL;
-- Context is keyed to the exact job or scan step, never joined by title or to
-- the newest knowledge version. Only recorded source metadata is exposed.
-- Overview counts must not traverse the detailed evidence/legacy joins. Only
-- aggregate state is exposed here; source configuration and content stay private.
CREATE OR REPLACE VIEW :"observer_schema".processing_runtime_counts AS
 WITH current_jobs AS (
  SELECT id,kind FROM :"app_schema".processing_jobs WHERE is_current
 ), stages AS (
  SELECT
   count(*) FILTER (WHERE s.status='running' AND s.lease_expires_at>NOW())::bigint AS executing,
   count(*) FILTER (WHERE s.status IN ('queued','enqueue_pending'))::bigint AS queued,
   count(*) FILTER (WHERE s.status IN ('waiting_external','retry_wait'))::bigint AS waiting,
   count(*) FILTER (WHERE s.status IN ('failed','blocked') OR
     (s.status='running' AND (s.lease_expires_at IS NULL OR s.lease_expires_at<=NOW())))::bigint AS abnormal
  FROM :"app_schema".processing_steps s JOIN current_jobs j ON j.id=s.job_id
  WHERE j.kind<>'legacy'
 ), jobs AS (
  SELECT count(*) FILTER (WHERE kind='document')::bigint AS documents,
         count(*) FILTER (WHERE kind='scan')::bigint AS batches FROM current_jobs
 ) SELECT stages.*,jobs.* FROM stages CROSS JOIN jobs;
CREATE OR REPLACE VIEW :"observer_schema".processing_job_context AS
 SELECT j.tenant_id,j.id AS job_id,COALESCE(t.name,j.tenant_id::text) AS workspace_name,
 ds.type AS source_type,NULLIF(j.metadata->>'file_id','') AS file_id,
 CASE WHEN j.kind='document' THEN NULLIF(j.metadata->>'title','') END AS document_title,
 CASE WHEN j.kind='document' AND ds.type='tencent_docs' AND NULLIF(j.metadata->>'title','') IS NOT NULL
      THEN concat_ws('/',NULLIF(j.metadata->>'folder_path',''),j.metadata->>'title') END AS source_path,
 CASE WHEN j.metadata->>'url' ~ '^https://docs[.]qq[.]com/[A-Za-z0-9_./%~-]+([?](resourceId|mode)=[A-Za-z0-9_%~-]+(&(resourceId|mode)=[A-Za-z0-9_%~-]+)*)?$'
      THEN j.metadata->>'url' END AS source_url,
 CASE WHEN file_result.bytes ~ '^[0-9]{1,18}$' THEN file_result.bytes::bigint END AS file_bytes,
 CASE WHEN file_result.bytes ~ '^[0-9]{1,18}$' THEN file_result.basis END AS size_basis,
 (SELECT s.id FROM :"app_schema".processing_steps s WHERE s.job_id=j.id
  AND s.status IN ('failed','blocked','retry_wait','running','waiting_external','enqueue_pending','queued')
  ORDER BY CASE s.status WHEN 'failed' THEN 0 WHEN 'blocked' THEN 1 WHEN 'retry_wait' THEN 2 WHEN 'running' THEN 3 ELSE 4 END,s.created_at,s.id LIMIT 1) AS active_step_id
 FROM :"app_schema".processing_jobs j
 LEFT JOIN :"app_schema".data_sources ds ON ds.id=j.datasource_id AND ds.tenant_id=j.tenant_id AND ds.knowledge_base_id=j.knowledge_base_id
 LEFT JOIN :"app_schema".tenants t ON t.id=j.tenant_id
 LEFT JOIN LATERAL (
   SELECT s.result->>'bytes' AS bytes,
    CASE s.stage WHEN 'download' THEN '本版本完整导出/下载文件' ELSE '已核验旧版本原文件' END AS basis
   FROM :"app_schema".processing_steps s WHERE s.job_id=j.id AND s.unit_key='body'
    AND s.stage IN ('download','legacy_snapshot') AND s.status='succeeded'
   ORDER BY CASE s.stage WHEN 'download' THEN 0 ELSE 1 END,s.id LIMIT 1
 ) file_result ON true;
CREATE OR REPLACE VIEW :"observer_schema".processing_step_context AS
 SELECT j.tenant_id,s.job_id,s.id AS step_id,s.stage,s.step_attempt,
 CASE WHEN s.error_code ~ '^[A-Z0-9_]{1,80}$' THEN s.error_code ELSE '' END AS error_code,
 CASE WHEN s.error_class ~ '^[a-zA-Z0-9_]{1,64}$' THEN s.error_class ELSE '' END AS error_class,
 CASE WHEN j.kind='scan' AND s.stage='scan_document' THEN NULLIF(s.input->>'title','') END AS document_title,
 CASE WHEN j.kind='scan' AND s.stage='scan_document' THEN NULLIF(s.input->>'file_id','') END AS file_id,
 CASE WHEN j.kind='scan' AND s.stage='scan_document' AND NULLIF(s.input->>'title','') IS NOT NULL
      THEN concat_ws('/',NULLIF(s.input->>'folder_path',''),s.input->>'title') END AS source_path,
 CASE WHEN j.kind='scan' AND s.stage='scan_document' AND s.input->>'url' ~ '^https://docs[.]qq[.]com/[A-Za-z0-9_./%~-]+([?](resourceId|mode)=[A-Za-z0-9_%~-]+(&(resourceId|mode)=[A-Za-z0-9_%~-]+)*)?$'
      THEN s.input->>'url' END AS source_url,
 CASE WHEN s.result->>'actual_bytes' ~ '^[0-9]{1,18}$' THEN (s.result->>'actual_bytes')::bigint END AS actual_bytes,
 CASE WHEN s.result->>'limit_bytes' ~ '^[0-9]{1,18}$' THEN (s.result->>'limit_bytes')::bigint END AS limit_bytes,
 CASE WHEN s.result->>'observed_at_least_bytes' ~ '^[0-9]{1,18}$' THEN (s.result->>'observed_at_least_bytes')::bigint END AS observed_at_least_bytes
 FROM :"app_schema".processing_steps s JOIN :"app_schema".processing_jobs j ON j.id=s.job_id;
CREATE OR REPLACE VIEW :"observer_schema".processing_legacy_context AS
 SELECT e.tenant_id,e.datasource_id,e.run_id,e.error_ordinal,e.error_digest,
 NULLIF(item.value->>'source_path','') AS source_path,
 CASE WHEN item.value->>'actual_bytes' ~ '^[0-9]{1,18}$' THEN (item.value->>'actual_bytes')::bigint END AS actual_bytes,
 CASE WHEN item.value->>'limit_bytes' ~ '^[0-9]{1,18}$' THEN (item.value->>'limit_bytes')::bigint END AS limit_bytes,
 CASE WHEN item.value->>'observed_at_least_bytes' ~ '^[0-9]{1,18}$' THEN (item.value->>'observed_at_least_bytes')::bigint END AS observed_at_least_bytes
 FROM :"app_schema".mwe_processing_legacy_error_state e
 JOIN :"app_schema".sync_logs s ON s.id=e.run_id AND s.tenant_id=e.tenant_id AND s.data_source_id=e.datasource_id
 CROSS JOIN LATERAL (SELECT s.result->'errors'->(e.error_ordinal::int-1) AS value) item;
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
