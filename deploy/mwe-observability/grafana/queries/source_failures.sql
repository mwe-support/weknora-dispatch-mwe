WITH latest AS (
  SELECT DISTINCT ON (sl.data_source_id) sl.*, ds.name AS source_name,ds.type AS source_type,
    kb.name AS kb_name, COALESCE(t.name,ds.tenant_id::text) AS workspace
  FROM sync_logs sl
  JOIN data_sources ds ON ds.id=sl.data_source_id AND ds.tenant_id=sl.tenant_id
    AND ds.deleted_at IS NULL AND ds.status<>'deleted'
  JOIN knowledge_bases kb ON kb.id=ds.knowledge_base_id AND kb.tenant_id=ds.tenant_id AND kb.deleted_at IS NULL
  LEFT JOIN tenants t ON t.id=ds.tenant_id
  ORDER BY sl.data_source_id,sl.updated_at DESC,sl.id DESC
), failure_rows AS (
  SELECT * FROM latest WHERE status IN ('failed','partial')
)
