WITH valid_sources AS (
  SELECT ds.id, ds.tenant_id, ds.knowledge_base_id, ds.name, ds.type,
         kb.name AS kb_name, COALESCE(t.name, ds.tenant_id::text) AS workspace
  FROM data_sources ds
  JOIN knowledge_bases kb ON kb.id=ds.knowledge_base_id AND kb.tenant_id=ds.tenant_id AND kb.deleted_at IS NULL
  LEFT JOIN tenants t ON t.id=ds.tenant_id
  WHERE ds.deleted_at IS NULL AND ds.status<>'deleted'
), raw_file_errors AS (
  SELECT ds.*, sl.id AS sync_log_id, e.ordinality AS error_index,
         COALESCE(NULLIF(e.item->>'title',''), '（历史错误未记录标题）') AS title,
         NULLIF(e.item->>'external_id','') AS external_id,
         NULLIF(e.item->>'file_id','') AS file_id,
         CASE WHEN pg_input_is_valid(e.item->>'occurred_at','timestamp with time zone')
              THEN (e.item->>'occurred_at')::timestamptz
              ELSE COALESCE(sl.updated_at,sl.finished_at,sl.started_at) AT TIME ZONE 'UTC' END AS failed_at,
         COALESCE(NULLIF(e.item->>'message',''), CASE WHEN jsonb_typeof(e.item)='string' THEN e.item #>> '{}' END, '未记录错误信息') AS message,
         e.item
  FROM sync_logs sl
  JOIN valid_sources ds ON ds.id=sl.data_source_id AND ds.tenant_id=sl.tenant_id AND ds.type='tencent_docs'
  CROSS JOIN LATERAL jsonb_array_elements(CASE WHEN jsonb_typeof(sl.result->'errors')='array' THEN sl.result->'errors' ELSE '[]'::jsonb END)
    WITH ORDINALITY AS e(item,ordinality)
), file_aliases AS (
  SELECT id, external_id, max(file_id) AS file_id FROM raw_file_errors
  WHERE external_id IS NOT NULL AND file_id IS NOT NULL
  GROUP BY id,external_id HAVING count(DISTINCT file_id)=1
), keyed_file_errors AS (
  SELECT e.*, COALESCE(e.file_id,a.file_id) AS canonical_file_id,
         CASE WHEN COALESCE(e.file_id,a.file_id) IS NOT NULL THEN 'file:'||COALESCE(e.file_id,a.file_id)
              WHEN e.external_id IS NOT NULL THEN 'external:'||e.external_id
              WHEN e.item->>'title' IS NOT NULL AND e.item->>'title'<>'' THEN 'legacy-title:'||e.title
              ELSE 'legacy-log:'||e.sync_log_id||':'||e.error_index::text END AS file_key
  FROM raw_file_errors e LEFT JOIN file_aliases a ON a.id=e.id AND a.external_id=e.external_id
), latest_file_errors AS (
  SELECT DISTINCT ON (id,file_key) * FROM keyed_file_errors
  ORDER BY id,file_key,failed_at DESC,sync_log_id DESC,error_index DESC
), unresolved_file_errors AS (
  SELECT e.* FROM latest_file_errors e
  WHERE NOT EXISTS (
    SELECT 1 FROM sync_logs completed
    WHERE completed.data_source_id=e.id AND completed.tenant_id=e.tenant_id
      AND e.external_id IS NOT NULL
      AND CASE WHEN pg_input_is_valid(completed.result->'faq_completed'->>e.external_id,'timestamp with time zone')
          THEN (completed.result->'faq_completed'->>e.external_id)::timestamptz>e.failed_at ELSE false END
  ) AND NOT EXISTS (
    SELECT 1 FROM knowledges k
    WHERE k.tenant_id=e.tenant_id AND k.knowledge_base_id=e.knowledge_base_id
      AND k.deleted_at IS NULL AND k.metadata->>'datasource_id'=e.id
      AND k.parse_status='completed'
	  AND COALESCE(k.metadata->>'datasource_candidate','')<>'true'
	  AND COALESCE(k.metadata->>'datasource_processing_failed','')=''
      AND ((e.canonical_file_id IS NOT NULL AND k.metadata->>'file_id'=e.canonical_file_id)
        OR (e.canonical_file_id IS NULL AND e.external_id IS NOT NULL AND k.metadata->>'external_id'=e.external_id))
      -- Updated timestamps or an old completed row are NOT source-read proof.
      -- A newly created matching row is historical evidence of re-ingestion.
      AND (k.created_at>e.failed_at OR
        CASE WHEN pg_input_is_valid(k.metadata->>'source_fetch_completed_at','timestamp with time zone')
             THEN (k.metadata->>'source_fetch_completed_at')::timestamptz>e.failed_at ELSE false END)
  )
), sync_failures AS (
  SELECT e.failed_at, e.tenant_id, e.workspace, e.kb_name, e.name AS ds_name,
         COALESCE(e.item->>'space_id','') AS space, e.title,
         COALESCE(NULLIF(e.item->>'stage',''),
           CASE WHEN e.message ~ 'download Tencent Docs export|FILE_SIZE_EXCEEDED' THEN 'download'
                WHEN e.message ~ 'export' THEN 'export'
                WHEN e.message ~ 'file info|query_file_info' THEN 'fetch_metadata' ELSE 'fetch' END) AS stage,
         CASE WHEN e.item->>'category'='FILE_SIZE_EXCEEDED' OR e.item->>'code'='FILE_SIZE_EXCEEDED'
                   OR e.message ~ 'download Tencent Docs export exceeds [0-9]+ bytes'
              THEN 'FILE_SIZE_EXCEEDED'
              ELSE COALESCE(NULLIF(e.item->>'category',''),NULLIF(e.item->>'code',''),'SOURCE_FETCH_FAILED') END AS category,
         CASE WHEN e.message ~ 'download Tencent Docs export exceeds [0-9]+ bytes'
              THEN '文件大小超出上限；历史日志未记录实际大小（不是解析引擎故障）' ELSE e.message END AS reason,
         e.sync_log_id, e.id AS data_source_id, NULL::text AS knowledge_id, e.knowledge_base_id,
         e.external_id, e.canonical_file_id AS file_id, e.type AS source,
         COALESCE(e.item->>'limit_bytes',substring(e.message from 'download Tencent Docs export exceeds ([0-9]+) bytes')) AS limit_bytes,
         e.item->>'actual_bytes' AS actual_bytes, e.item->>'observed_at_least_bytes' AS observed_at_least_bytes,
         CASE WHEN e.canonical_file_id IS NOT NULL THEN 'file_id' WHEN e.external_id IS NOT NULL THEN 'external_id' ELSE 'legacy_title_only' END AS identity_basis,
         CASE WHEN e.canonical_file_id IS NULL AND e.external_id IS NULL THEN '恢复待核实：历史记录缺少稳定文件ID' ELSE '未解决：尚无后续重新抓取并完成的证据' END AS resolution,
         e.file_key, NULLIF(e.item->>'source_path','') AS source_path
  FROM unresolved_file_errors e
), knowledge_failures AS (
  SELECT k.updated_at AS failed_at, k.tenant_id, COALESCE(t.name,k.tenant_id::text) AS workspace,
         kb.name AS kb_name, COALESCE(ds.name,'手工/API上传') AS ds_name,
         COALESCE(NULLIF(k.metadata->>'space_name',''),NULLIF(k.metadata->>'space_id',''),'') AS space,
         COALESCE(NULLIF(k.title,''),NULLIF(k.file_name,''),k.id) AS title,
         COALESCE(sp.name,'knowledge_processing') AS stage,
         COALESCE(NULLIF(sp.error_code,''),'KNOWLEDGE_PROCESSING_FAILED') AS category,
         COALESCE(NULLIF(sp.error_message,''),NULLIF(k.error_message,''),'未记录错误信息') AS reason,
         NULL::text AS sync_log_id, COALESCE(ds.id,'') AS data_source_id, k.id AS knowledge_id, k.knowledge_base_id,
         NULLIF(k.metadata->>'external_id','') AS external_id, NULLIF(k.metadata->>'file_id','') AS file_id,
         COALESCE(ds.type,k.channel) AS source,
         NULL::text AS limit_bytes, CASE WHEN k.file_size>0 THEN k.file_size::text END AS actual_bytes, NULL::text AS observed_at_least_bytes,
         'knowledge_id'::text AS identity_basis, '未解决：知识处理失败'::text AS resolution,
         CASE WHEN NULLIF(k.metadata->>'file_id','') IS NOT NULL THEN 'file:'||(k.metadata->>'file_id')
              WHEN NULLIF(k.metadata->>'external_id','') IS NOT NULL THEN 'external:'||(k.metadata->>'external_id')
              ELSE 'knowledge:'||k.id END AS file_key,
         CASE WHEN COALESCE(ds.type,k.channel)='tencent_docs' THEN NULLIF(k.metadata->>'source_path','') END AS source_path
  FROM knowledges k
  JOIN knowledge_bases kb ON kb.id=k.knowledge_base_id AND kb.tenant_id=k.tenant_id AND kb.deleted_at IS NULL
  LEFT JOIN tenants t ON t.id=k.tenant_id
  LEFT JOIN valid_sources ds ON ds.id=k.metadata->>'datasource_id' AND ds.tenant_id=k.tenant_id AND ds.knowledge_base_id=k.knowledge_base_id
  LEFT JOIN LATERAL (SELECT s.name,s.error_code,s.error_message FROM knowledge_processing_spans s
      WHERE s.knowledge_id=k.id AND s.status='failed' ORDER BY s.finished_at DESC NULLS LAST,s.updated_at DESC LIMIT 1) sp ON true
  WHERE k.parse_status='failed' AND k.deleted_at IS NULL
    AND (NULLIF(k.metadata->>'datasource_id','') IS NULL OR ds.id IS NOT NULL)
), all_failures AS (
  SELECT * FROM sync_failures UNION ALL SELECT * FROM knowledge_failures
), linked_failures AS (
  SELECT f.*, max(sync_log_id) OVER file_group AS linked_sync_log_id,
         max(knowledge_id) OVER file_group AS linked_knowledge_id
  FROM all_failures f WINDOW file_group AS (PARTITION BY tenant_id,knowledge_base_id,data_source_id,file_key)
), failure_rows AS (
  SELECT DISTINCT ON (tenant_id,knowledge_base_id,data_source_id,file_key) * FROM linked_failures
  ORDER BY tenant_id,knowledge_base_id,data_source_id,file_key,failed_at DESC,knowledge_id NULLS LAST
)
