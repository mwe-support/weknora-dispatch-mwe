-- All pages use this one result set. Above the bound, return an explicit
-- lower-bound notice, never a truncated list with a fabricated complete count.
WITH matching AS (
 SELECT c.*,
 COALESCE(NULLIF(sc.document_title,''),NULLIF(jc.document_title,''),NULLIF(c.title,''),NULLIF(c.source_name,''),'未记录文档名称') AS display_title,
 COALESCE(sc.source_path,jc.source_path,lc.source_path,k.metadata->>'source_path') AS display_source_path,
 COALESCE(sc.file_id,jc.file_id,k.metadata->>'file_id') AS display_file_id,
 COALESCE(sc.source_url,jc.source_url) AS display_source_url,
 COALESCE(jc.source_type,ds.type) AS display_source_type,
 COALESCE(jc.workspace_name,t.name,c.tenant_id::text) AS display_workspace,
 COALESCE(jc.file_bytes,CASE WHEN TRUE AND sc.stage='download' THEN sc.actual_bytes END,lc.actual_bytes,CASE WHEN k.file_size>0 THEN k.file_size END) AS display_file_bytes,
 COALESCE(jc.size_basis,CASE WHEN TRUE AND sc.stage='download' AND sc.actual_bytes IS NOT NULL THEN '本次下载失败记录' END,CASE WHEN lc.actual_bytes IS NOT NULL THEN '原始失败记录' END,CASE WHEN k.file_size>0 THEN '旧知识文件记录' END,CASE WHEN c.native_raw_bytes IS NOT NULL THEN '原生接口响应；不是导入文件大小' ELSE '未记录；不使用配额、上限或旧版本大小替代' END) AS display_size_basis,
 COALESCE(CASE WHEN TRUE THEN sc.limit_bytes END,lc.limit_bytes) AS display_limit_bytes,
 COALESCE(CASE WHEN TRUE THEN sc.observed_at_least_bytes END,lc.observed_at_least_bytes) AS display_lower_bytes,
 COALESCE(NULL::text,sc.error_code,'') AS display_error_code
 FROM mwe_processing_current_document_lifecycle c
 LEFT JOIN processing_job_context jc ON jc.tenant_id=c.tenant_id AND jc.job_id=c.job_id
 LEFT JOIN processing_step_context sc ON sc.tenant_id=c.tenant_id AND sc.job_id=c.job_id AND sc.step_id=jc.active_step_id
 LEFT JOIN processing_legacy_context lc ON lc.tenant_id=c.tenant_id AND lc.datasource_id=c.datasource_id AND lc.run_id=c.run_id AND lc.error_ordinal=c.original_error_ordinal AND lc.error_digest=c.original_error_digest
 LEFT JOIN knowledges k ON COALESCE(c.job_id,'')='' AND c.original_error_ordinal IS NULL AND k.id=c.knowledge_id AND k.tenant_id=c.tenant_id AND k.knowledge_base_id=c.knowledge_base_id AND k.metadata->>'datasource_id'=c.datasource_id
 LEFT JOIN tenants t ON t.id=c.tenant_id
 LEFT JOIN data_sources ds ON ds.id=c.datasource_id AND ds.tenant_id=c.tenant_id AND ds.knowledge_base_id=c.knowledge_base_id
 WHERE (${workspace:sqlstring}='' OR c.tenant_id::text=${workspace:sqlstring}) AND (${knowledge_base:sqlstring}='' OR c.knowledge_base_id::text=${knowledge_base:sqlstring}) AND (${source:sqlstring}='' OR c.datasource_id::text=${source:sqlstring}) AND (${run:sqlstring}='' OR c.run_id::text=${run:sqlstring}) AND (${status:sqlstring} IN ('','all') OR c.status::text=${status:sqlstring}) AND (${stage:sqlstring}='' OR c.stage::text=${stage:sqlstring}) AND (${records:sqlstring}='all' OR (${records:sqlstring}='current' AND c.evidence_basis='verified_v2') OR (${records:sqlstring}='legacy' AND c.evidence_basis<>'verified_v2')) AND (${search:sqlstring}='' OR position(lower(${search:sqlstring}) in lower(concat_ws(' ',sc.document_title,c.title,c.external_id,c.job_id,sc.file_id,jc.file_id,sc.source_path,jc.source_path,lc.source_path,k.metadata->>'source_path')))>0)
), candidates AS MATERIALIZED (
 SELECT * FROM matching
 ORDER BY observed_at DESC,row_id DESC LIMIT 10001
), gate AS (SELECT COUNT(*) AS n FROM candidates)
SELECT display_title AS "文档名称",
 COALESCE(NULLIF(display_source_path,''),CASE WHEN display_source_type='tencent_docs' THEN '未记录' ELSE '不适用' END) AS "腾讯文档路径",
 CASE WHEN display_file_bytes IS NULL AND native_raw_bytes IS NOT NULL THEN CASE WHEN native_raw_bytes::text ~ '^[0-9]{1,18}$' THEN
      CASE WHEN (native_raw_bytes::text)::numeric>=1073741824 THEN round((native_raw_bytes::text)::numeric/1073741824,2)::text||' GB'
           WHEN (native_raw_bytes::text)::numeric>=1048576 THEN round((native_raw_bytes::text)::numeric/1048576,2)::text||' MB'
           ELSE round((native_raw_bytes::text)::numeric/1024,2)::text||' KB' END ELSE '未记录' END||'（响应）' ELSE CASE WHEN display_file_bytes::text ~ '^[0-9]{1,18}$' THEN
      CASE WHEN (display_file_bytes::text)::numeric>=1073741824 THEN round((display_file_bytes::text)::numeric/1073741824,2)::text||' GB'
           WHEN (display_file_bytes::text)::numeric>=1048576 THEN round((display_file_bytes::text)::numeric/1048576,2)::text||' MB'
           ELSE round((display_file_bytes::text)::numeric/1024,2)::text||' KB' END ELSE '未记录' END END AS "文件 / 响应大小",
 display_workspace||E'\n'||kb_name AS "工作空间 / 知识库",
 CASE stage WHEN 'scan' THEN '扫描批次' WHEN 'legacy_scan' THEN '历史扫描' WHEN 'scan_page' THEN '扫描目录' WHEN 'scan_document' THEN '确认源文档' WHEN 'native_read' THEN '读取源内容' WHEN 'export_start' THEN '发起导出' WHEN 'export_poll' THEN '等待导出' WHEN 'download' THEN '下载文件' WHEN 'normalize' THEN '内容转换' WHEN 'parse' THEN '解析' WHEN 'assets' THEN '收集图片' WHEN 'images' THEN '图片处理' WHEN 'image_ocr' THEN '图片文字识别' WHEN 'image_caption' THEN '图片描述' WHEN 'chunk' THEN '文档分块' WHEN 'embedding' THEN '生成向量' WHEN 'text_index' THEN '正文索引' WHEN 'image_index' THEN '图片索引' WHEN 'summary' THEN '摘要' WHEN 'questions' THEN '生成问题' WHEN 'publish' THEN '发布版本' WHEN 'retire' THEN '清理版本' WHEN 'retire_previous' THEN '清理旧版本' WHEN 'legacy_snapshot' THEN '核验旧文件' WHEN 'legacy_indexes' THEN '核验旧索引' WHEN 'question' THEN '生成单条问题' WHEN 'question_index' THEN '问题索引' WHEN 'summary_index' THEN '摘要索引' WHEN 'index' THEN '索引写入' ELSE COALESCE(NULLIF(stage,''),'—') END AS "处理阶段",
 CASE status WHEN 'planned' THEN '待启动' WHEN 'enqueue_pending' THEN '待派发' WHEN 'queued' THEN '排队中' WHEN 'running' THEN '处理中' WHEN 'waiting_external' THEN '等待外部结果' WHEN 'retry_wait' THEN '等待重试' WHEN 'succeeded' THEN '已完成' WHEN 'success' THEN '历史成功' WHEN 'completed' THEN '历史完成' WHEN 'partial' THEN '历史部分成功' WHEN 'failed' THEN '失败' WHEN 'blocked' THEN '需处理' WHEN 'canceled' THEN '已取消' WHEN 'superseded' THEN '已替代' WHEN 'skipped' THEN '已跳过' WHEN 'legacy_unverified' THEN '历史待核实' ELSE COALESCE(NULLIF(status,''),'—') END AS "状态",
 CASE WHEN steps_total IS NULL THEN '历史阶段未核实' ELSE steps_succeeded::text||' / '||steps_total::text END AS "阶段进度",
 CASE availability WHEN 'current_available' THEN '当前版本可用' WHEN 'previous_available' THEN '旧版本仍可用' WHEN 'not_published' THEN '尚未发布' WHEN 'legacy_available' THEN '历史版本可用' WHEN 'legacy_unverified' THEN '历史待核实' ELSE COALESCE(NULLIF(availability,''),'—') END AS "可用版本",
 observed_at AS "记录时间",
 display_source_url AS "腾讯原文",
 jsonb_build_object('job_id',job_id, 'knowledge_base_id',knowledge_base_id, 'datasource_id',datasource_id, 'source_revision',source_revision, 'revision',revision, 'publication_epoch',publication_epoch, 'availability',availability, 'evidence_basis',evidence_basis, 'published_job_id',published_job_id, 'published_generation',published_generation, 'doc_type',doc_type, 'native_raw_bytes',native_raw_bytes, 'native_pages',native_pages, 'native_units',native_units, 'images',images, 'media_bytes',media_bytes, 'readiness',readiness, 'completeness',completeness, 'steps_total',steps_total, 'steps_succeeded',steps_succeeded, 'cleanup_problem',cleanup_problem, 'resources_active',resources_active, 'charged_bytes',charged_bytes, 'estimated_index_bytes',estimated_index_bytes, 'run_id',run_id, 'original_error_ordinal',original_error_ordinal, 'original_error_digest',original_error_digest, 'linked_job_id',linked_job_id, 'legacy_action',legacy_action, 'legacy_actor',legacy_actor, 'legacy_evidence_at',legacy_evidence_at, 'legacy_dead_letter_id',legacy_dead_letter_id, 'legacy_run_status',legacy_run_status, '腾讯文档路径',display_source_path, '腾讯文件ID',display_file_id, '来源类型',display_source_type, '原文字节',display_file_bytes, '大小依据',display_size_basis, '原生响应字节',native_raw_bytes, '大小上限',display_limit_bytes, '至少已读取字节',display_lower_bytes)::text AS "详情"
FROM candidates c CROSS JOIN gate WHERE gate.n<=10000
UNION ALL
SELECT '至少10001条，请缩小筛选范围','','','','','','','',NULL::timestamptz,'','' FROM gate WHERE gate.n>10000;
