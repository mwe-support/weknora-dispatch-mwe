-- All pages use this one result set. Above the bound, return an explicit
-- lower-bound notice, never a truncated list with a fabricated complete count.
WITH candidates AS MATERIALIZED (
 SELECT * FROM mwe_processing_current_run_progress WHERE (${workspace:sqlstring}='' OR tenant_id::text=${workspace:sqlstring}) AND (${knowledge_base:sqlstring}='' OR knowledge_base_id::text=${knowledge_base:sqlstring}) AND (${source:sqlstring}='' OR datasource_id::text=${source:sqlstring}) AND (${run:sqlstring}='' OR run_id::text=${run:sqlstring}) AND (${status:sqlstring}='' OR status::text=${status:sqlstring}) AND (${stage:sqlstring}='' OR stage::text=${stage:sqlstring}) AND (${search:sqlstring}='' OR position(lower(${search:sqlstring}) in lower(title||' '||external_id||' '||job_id))>0)
 ORDER BY observed_at DESC,row_id DESC LIMIT 10001
), gate AS (SELECT COUNT(*) AS n FROM candidates)
SELECT '完整快照：'||gate.n::text||'条' AS "查询状态", c.title AS "文档或批次",
 c.tenant_id::text AS "工作空间ID", c.kb_name AS "知识库", c.source_name AS "数据源",
 c.generation::text AS "文档版本", c.status AS "状态", c.stage AS "阶段", c.observed_at::text AS "记录时间",
 jsonb_build_object('job_id',job_id, 'knowledge_base_id',knowledge_base_id, 'datasource_id',datasource_id, 'source_revision',source_revision, 'revision',revision, 'publication_epoch',publication_epoch, 'documents',documents, 'documents_succeeded',documents_succeeded, 'documents_failed',documents_failed, 'documents_blocked',documents_blocked, 'documents_active',documents_active, 'documents_unadmitted',documents_unadmitted, 'containers',containers, 'external_links',external_links, 'original_finished_status',original_finished_status, 'original_finished_at',original_finished_at, 'run_id',run_id, 'original_error_ordinal',original_error_ordinal, 'original_error_digest',original_error_digest, 'linked_job_id',linked_job_id, 'legacy_action',legacy_action, 'legacy_actor',legacy_actor, 'legacy_evidence_at',legacy_evidence_at, 'legacy_dead_letter_id',legacy_dead_letter_id, 'legacy_run_status',legacy_run_status)::text AS "详情"
FROM candidates c CROSS JOIN gate WHERE gate.n<=10000
UNION ALL
SELECT '至少10001条，请缩小筛选范围','','','','','','','','','' FROM gate WHERE gate.n>10000;
