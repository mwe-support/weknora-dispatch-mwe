-- All pages use this one result set. Above the bound, return an explicit
-- lower-bound notice, never a truncated list with a fabricated complete count.
WITH candidates AS MATERIALIZED (
 SELECT * FROM mwe_processing_lifecycle_inconsistencies WHERE (${workspace:sqlstring}='' OR tenant_id::text=${workspace:sqlstring}) AND (${knowledge_base:sqlstring}='' OR knowledge_base_id::text=${knowledge_base:sqlstring}) AND (${source:sqlstring}='' OR datasource_id::text=${source:sqlstring}) AND (${run:sqlstring}='' OR run_id::text=${run:sqlstring}) AND (${status:sqlstring}='' OR status::text=${status:sqlstring}) AND (${stage:sqlstring}='' OR stage::text=${stage:sqlstring}) AND (${search:sqlstring}='' OR position(lower(${search:sqlstring}) in lower(title||' '||external_id||' '||job_id))>0)
 ORDER BY observed_at DESC,row_id DESC LIMIT 10001
), gate AS (SELECT COUNT(*) AS n FROM candidates)
SELECT '完整快照：'||gate.n::text||'条' AS "查询状态", c.title AS "文档或批次",
 c.tenant_id::text AS "工作空间ID", c.kb_name AS "知识库", c.source_name AS "数据源",
 c.generation::text AS "文档版本", c.status AS "状态", c.stage AS "阶段", c.observed_at::text AS "记录时间",
 jsonb_build_object('job_id',job_id, 'knowledge_base_id',knowledge_base_id, 'datasource_id',datasource_id, 'source_revision',source_revision, 'revision',revision, 'publication_epoch',publication_epoch, 'step_id',step_id, 'readiness',readiness, 'completeness',completeness, 'retirement_state',retirement_state, 'is_current',is_current, 'is_published',is_published, 'cleanup_problem',cleanup_problem)::text AS "详情"
FROM candidates c CROSS JOIN gate WHERE gate.n<=10000
UNION ALL
SELECT '至少10001条，请缩小筛选范围','','','','','','','','','' FROM gate WHERE gate.n>10000;
