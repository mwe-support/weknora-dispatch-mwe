"""One safe projection contract for the API and Grafana, in both SQL dialects."""
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
VIEWS = ['job_state', 'current_document_lifecycle', 'current_run_progress', 'stage_retry_queue',
         'attempt_timeline', 'unresolved_incidents', 'lifecycle_inconsistencies']
LEGACY_VIEWS = ['legacy_run_state', 'legacy_error_state', 'legacy_evidence_state']
JOB_COLUMNS = '''job_id tenant_id knowledge_base_id datasource_id external_id run_id knowledge_id kind generation source_revision
configuration_revision pipeline_fingerprint revision is_current is_published publication_epoch rollback_pin retirement_state readiness completeness job_status plan_sealed
title folder_path source_name kb_name created_at updated_at published_at finished_at steps_total steps_succeeded steps_problem cleanup_problem next_retry_at heartbeat_at
resources_active charged_bytes estimated_index_bytes doc_type native_raw_bytes native_pages native_units images media_bytes active_stage'''.split()
TIMELINE_COLUMNS = JOB_COLUMNS + '''row_id event_id step_id stage event_type status event_revision step_attempt dispatch_seq from_state error_class error_code
actor action operation_request_id queue_task_id trace_id resolves_event_id resolution_type observed_at'''.split()


def legacy_views(dialect, js):
    pg = dialect == 'versioned'
    def safe(value, pattern, size):
        check = f"{value} ~ '^{pattern}{{1,{size}}}$'" if pg else f"LENGTH({value}) BETWEEN 1 AND {size} AND {value} NOT GLOB '*[^{pattern[1:-1]}]*'"
        return f"CASE WHEN {check} THEN {value} ELSE '' END"
    errors = ("jsonb_array_elements(CASE WHEN jsonb_typeof(r.result->'errors')='array' THEN r.result->'errors' ELSE '[]'::jsonb END) WITH ORDINALITY e(item, ordinal)" if pg
              else "json_each(CASE WHEN json_type(r.result,'$.errors')='array' THEN json_extract(r.result,'$.errors') ELSE '[]' END) e")
    item, ordinal = ('e.item', 'e.ordinal') if pg else ('e.value', '(CAST(e.key AS INTEGER)+1)')
    # Bare-string legacy errors remain separate rows but have no claimed file identity.
    value = lambda key: (js(item, key) if pg else f"CASE WHEN e.type='object' THEN {js(item,key)} ELSE '' END")
    digest = f"encode(sha256(convert_to({item}::text,'UTF8')),'hex')" if pg else f"weknora_sha256(json_quote(json_extract(r.result,'$.errors['||e.key||']')))"
    occurred = value('occurred_at')
    occurred = (f"CASE WHEN pg_input_is_valid({occurred},'timestamp with time zone') THEN CAST({occurred} AS TIMESTAMPTZ) ELSE r.started_at END" if pg
                else f"CASE WHEN julianday({occurred}) IS NOT NULL THEN {occurred} ELSE r.started_at END")
    return f"""CREATE VIEW mwe_processing_legacy_run_state AS
SELECT s.id AS run_id,s.tenant_id,ds.knowledge_base_id,ds.id AS datasource_id,
 ds.name AS source_name,kb.name AS kb_name,s.status AS run_status,s.started_at,s.updated_at,s.finished_at,
 s.items_total,s.items_created,s.items_updated,s.items_skipped,s.items_failed,s.result,
 (SELECT MAX(d.id) FROM task_dead_letters d WHERE d.tenant_id=s.tenant_id AND d.task_type='datasource:sync'
 AND {js('d.payload','data_source_id')}=ds.id AND {js('d.payload','sync_log_id')}=s.id) AS dead_letter_id
FROM sync_logs s JOIN data_sources ds ON ds.id=s.data_source_id AND ds.tenant_id=s.tenant_id
LEFT JOIN knowledge_bases kb ON kb.id=ds.knowledge_base_id AND kb.tenant_id=s.tenant_id
WHERE ds.type='tencent_docs' AND NOT EXISTS (SELECT 1 FROM processing_jobs j WHERE j.kind='scan' AND j.tenant_id=s.tenant_id AND j.datasource_id=ds.id AND j.origin_run_id=s.id);

CREATE VIEW mwe_processing_legacy_error_state AS
SELECT r.run_id,r.tenant_id,r.knowledge_base_id,r.datasource_id,r.source_name,r.kb_name,r.run_status,r.started_at,r.updated_at,r.finished_at,r.dead_letter_id,
 {ordinal} AS error_ordinal,{digest} AS error_digest,
 COALESCE({value('external_id')},'') AS external_id,COALESCE({value('file_id')},'') AS file_id,
 COALESCE({value('title')},'') AS title,
 {safe(value('stage'),'[a-z0-9_]',64)} AS stage,
 {safe(value('code'),'[A-Z0-9_]',80)} AS error_code,
 {safe(value('category'),'[A-Z0-9_]',80)} AS error_class,
 CASE WHEN {value('retry_state')} IN ('scheduled','exhausted','failed','pending','running','completed') THEN {value('retry_state')} ELSE '' END AS retry_state,
 {occurred} AS observed_at
FROM mwe_processing_legacy_run_state r CROSS JOIN {errors};

CREATE VIEW mwe_processing_legacy_evidence_state AS
SELECT e.*,p.id AS evidence_id,p.action,p.actor,p.operation_request_id,p.job_id AS linked_job_id,p.knowledge_id,
 p.source_revision,p.attempt,p.created_at AS evidence_at
FROM mwe_processing_legacy_error_state e JOIN processing_legacy_evidence p
 ON p.tenant_id=e.tenant_id AND p.knowledge_base_id=e.knowledge_base_id AND p.datasource_id=e.datasource_id
 AND p.run_id=e.run_id AND p.error_ordinal=e.error_ordinal AND p.error_digest=e.error_digest;

"""


def legacy_union(name):
    # Explicit shared columns keep nullable legacy evidence out of v2 job identity.
    defaults = {key: 'NULL' for key in JOB_COLUMNS}
    defaults.update({key: "''" for key in ['job_id','knowledge_id','external_id','source_revision','configuration_revision','pipeline_fingerprint','folder_path']})
    defaults.update({key: 'FALSE' for key in ['is_current','is_published','rollback_pin','plan_sealed']})
    defaults.update({key: 'l.'+key for key in ['tenant_id','knowledge_base_id','datasource_id','run_id','source_name','kb_name']})
    defaults.update(kind="'legacy'",retirement_state="'legacy_unverified'",readiness="'unknown'",completeness="'unknown'",job_status='l.run_status',
                    created_at='l.started_at',updated_at='l.updated_at',finished_at='l.finished_at',title="l.source_name||' / '||l.run_id")
    def base(**fields):
        values = defaults | fields
        return ','.join(values[key] for key in JOB_COLUMNS)
    unresolved = """NOT EXISTS (SELECT 1 FROM mwe_processing_legacy_evidence_state p WHERE p.tenant_id=l.tenant_id AND p.run_id=l.run_id
 AND p.error_ordinal=l.error_ordinal AND p.error_digest=l.error_digest AND p.action IN ('late_completion','manual_confirmed','recovered','policy_skipped'))"""
    error_row = "'legacy-error:'||l.run_id||':'||CAST(l.error_ordinal AS TEXT)"
    error_base = lambda: base(external_id='l.external_id',title='l.title')
    if name == 'current_run_progress':
        return f"""SELECT {base()},'legacy-run:'||l.run_id,l.run_status,'legacy_scan',l.updated_at,
 NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,l.run_status,l.finished_at
FROM mwe_processing_legacy_run_state l"""
    if name == 'stage_retry_queue':
        return f"""SELECT {error_base()},{error_row},'',l.retry_state,l.stage,'legacy','',
 NULL,NULL,NULL,NULL,NULL,NULL,NULL,FALSE,FALSE,l.error_class,l.error_code,NULL,l.observed_at
FROM mwe_processing_legacy_error_state l WHERE l.retry_state='scheduled' AND {unresolved}"""
    if name in ('attempt_timeline','unresolved_incidents'):
        original = f"""SELECT {error_base()},{error_row},NULL,'',l.stage,'legacy_error','failed',NULL,
 NULL,NULL,'',l.error_class,CASE WHEN l.error_code<>'' THEN l.error_code ELSE 'LEGACY_UNVERIFIED' END,
 '','','','','',NULL,'',l.observed_at FROM mwe_processing_legacy_error_state l"""
        if name == 'unresolved_incidents':
            return original+' WHERE '+unresolved
        return original+f"""
UNION ALL
SELECT {base(external_id='l.external_id',title='l.title',knowledge_id='l.knowledge_id',source_revision='l.source_revision')},
 'legacy-evidence:'||l.evidence_id,NULL,'',l.stage,'legacy_evidence',l.action,NULL,
 l.attempt,NULL,'','','',l.actor,l.action,l.operation_request_id,'','',NULL,l.action,l.evidence_at
FROM mwe_processing_legacy_evidence_state l"""
    if name == 'lifecycle_inconsistencies':
        return f"""SELECT {base()},'legacy-dead-letter:'||CAST(l.dead_letter_id AS TEXT),'LEGACY_DEAD_LETTER_RUNNING','legacy_scan','',l.updated_at
FROM mwe_processing_legacy_run_state l WHERE l.run_status='running' AND l.dead_letter_id IS NOT NULL
UNION ALL
SELECT {error_base()},'legacy-scheduled:'||l.run_id||':'||CAST(l.error_ordinal AS TEXT),'LEGACY_SCHEDULED_UNVERIFIED',l.stage,'',l.observed_at
FROM mwe_processing_legacy_error_state l WHERE l.retry_state='scheduled' AND {unresolved}
 AND NOT EXISTS (SELECT 1 FROM mwe_processing_legacy_evidence_state p WHERE p.tenant_id=l.tenant_id AND p.run_id=l.run_id AND p.error_ordinal=l.error_ordinal AND p.error_digest=l.error_digest AND p.action='retry_admitted')"""
    return ''


def legacy_history_columns(name, sql):
    basis = '' if name == 'current_document_lifecycle' else ",CASE WHEN v.kind='legacy' THEN 'legacy_record' ELSE 'verified_v2' END AS evidence_basis"
    return f"""SELECT v.*{basis},e.error_ordinal AS original_error_ordinal,e.error_digest AS original_error_digest,
 p.linked_job_id,p.action AS legacy_action,p.actor AS legacy_actor,p.evidence_at AS legacy_evidence_at,
 l.dead_letter_id AS legacy_dead_letter_id,l.run_status AS legacy_run_status
FROM ({sql}) v
LEFT JOIN mwe_processing_legacy_run_state l ON v.kind='legacy' AND l.tenant_id=v.tenant_id AND l.run_id=v.run_id AND l.datasource_id=v.datasource_id
LEFT JOIN mwe_processing_legacy_error_state e ON l.tenant_id=e.tenant_id AND l.run_id=e.run_id AND
 (v.row_id IN ('legacy-error:'||e.run_id||':'||CAST(e.error_ordinal AS TEXT),'legacy-scheduled:'||e.run_id||':'||CAST(e.error_ordinal AS TEXT))
 OR EXISTS (SELECT 1 FROM mwe_processing_legacy_evidence_state event WHERE event.tenant_id=e.tenant_id AND event.run_id=e.run_id AND event.error_ordinal=e.error_ordinal AND v.row_id='legacy-evidence:'||event.evidence_id))
LEFT JOIN mwe_processing_legacy_evidence_state p ON p.evidence_id=(SELECT receipt.evidence_id FROM mwe_processing_legacy_evidence_state receipt
 WHERE receipt.tenant_id=e.tenant_id AND receipt.run_id=e.run_id AND receipt.error_ordinal=e.error_ordinal AND receipt.error_digest=e.error_digest
 AND (v.row_id NOT LIKE 'legacy-evidence:%' OR v.row_id='legacy-evidence:'||receipt.evidence_id)
 ORDER BY receipt.evidence_at DESC,receipt.evidence_id DESC LIMIT 1)"""


def render(dialect, legacy=False, tables=True):
    def age(column):
        return (f"EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - {column}))" if dialect == 'versioned'
                else f"(unixepoch('now') - unixepoch({column}))")

    def js(column, key):
        return (f"({column} ->> '{key}')" if dialect == 'versioned'
                else f"CAST(json_extract({column}, '$.{key}') AS TEXT)")

    def view(name, sql):
        if legacy:
            extra = legacy_union(name)
            if extra:
                sql += '\nUNION ALL\n'+extra
            if name != 'job_state':
                sql = legacy_history_columns(name,sql)
        return f"CREATE VIEW mwe_processing_{name} AS\n{sql};\n\n"

    def metric(stage, key):
        value = js('s.result', key)
        number_type = (f"jsonb_typeof(s.result->'{key}')='number'" if dialect == 'versioned'
                       else f"json_type(s.result, '$.{key}')='integer'")
        return f"(SELECT CASE WHEN {number_type} THEN CAST({value} AS BIGINT) ELSE NULL END FROM processing_steps s WHERE s.job_id=j.id AND s.stage='{stage}' AND s.unit_key='body' LIMIT 1)"

    text = "-- Generated by scripts/render-processing-history.py; edit the generator.\n"
    if legacy:
        text += legacy_views(dialect,js)
    text += view('job_state', f"""SELECT j.id AS job_id, j.tenant_id, j.knowledge_base_id,
 j.datasource_id, j.external_id, j.origin_run_id AS run_id, j.knowledge_id,
 j.kind, j.generation, j.source_revision, j.configuration_revision, j.pipeline_fingerprint,
 j.revision, j.is_current, j.is_published, j.publication_epoch, j.rollback_pin,
 j.retirement_state, j.readiness, j.completeness, j.status AS job_status, j.plan_sealed,
 COALESCE(NULLIF({js('j.metadata', 'title')}, ''), NULLIF(k.title, ''), j.external_id) AS title,
 COALESCE({js('j.metadata', 'folder_path')}, '') AS folder_path,
 COALESCE(ds.name, '') AS source_name, COALESCE(kb.name, '') AS kb_name,
 j.created_at, j.updated_at, j.published_at, j.finished_at,
 (SELECT COUNT(*) FROM processing_steps s WHERE s.job_id=j.id) AS steps_total,
 (SELECT COUNT(*) FROM processing_steps s WHERE s.job_id=j.id AND s.status='succeeded') AS steps_succeeded,
 (SELECT COUNT(*) FROM processing_steps s WHERE s.job_id=j.id AND s.status IN ('blocked','failed')) AS steps_problem,
 (SELECT COUNT(*) FROM processing_steps s WHERE s.job_id=j.id AND s.phase='retire' AND s.status IN ('blocked','failed','retry_wait')) AS cleanup_problem,
 (SELECT MIN(s.next_run_at) FROM processing_steps s WHERE s.job_id=j.id AND s.status IN ('retry_wait','waiting_external','enqueue_pending','queued')) AS next_retry_at,
 (SELECT MAX(s.heartbeat_at) FROM processing_steps s WHERE s.job_id=j.id) AS heartbeat_at,
 (SELECT COUNT(*) FROM resource_bindings b JOIN resources r ON r.id=b.resource_id AND r.tenant_id=b.tenant_id
  WHERE b.tenant_id=j.tenant_id AND b.owner_type='processing_job' AND b.owner_id=j.id AND r.state='active') AS resources_active,
 (SELECT COALESCE(SUM(r.bytes),0) FROM processing_storage_reservations r WHERE r.tenant_id=j.tenant_id AND r.job_id=j.id AND r.state <> 'released')
 + (SELECT COALESCE(SUM(w.estimated_bytes),0) FROM faq_index_writes w WHERE w.tenant_id=j.tenant_id AND w.job_id=j.id AND NOT w.storage_released) AS charged_bytes,
 (SELECT COALESCE(SUM(r.bytes),0) FROM processing_storage_reservations r WHERE r.tenant_id=j.tenant_id AND r.job_id=j.id AND r.kind='index' AND r.state <> 'released')
 + (SELECT COALESCE(SUM(w.estimated_bytes),0) FROM faq_index_writes w WHERE w.tenant_id=j.tenant_id AND w.job_id=j.id AND NOT w.storage_released) AS estimated_index_bytes,
 CASE WHEN {js('j.metadata', 'kind')} IN ('doc','smartcanvas','sheet','smartsheet','resource') THEN {js('j.metadata','kind')} ELSE NULL END AS doc_type,
 {metric('native_read', 'raw_bytes')} AS native_raw_bytes, {metric('native_read', 'pages')} AS native_pages,
 {metric('native_read', 'units')} AS native_units, {metric('assets','images')} AS images,
 {metric('assets','media_bytes')} AS media_bytes,
 (SELECT s.stage FROM processing_steps s WHERE s.job_id=j.id AND s.status IN ('failed','blocked','retry_wait','running','waiting_external','enqueue_pending','queued')
 ORDER BY CASE s.status WHEN 'failed' THEN 0 WHEN 'blocked' THEN 1 WHEN 'retry_wait' THEN 2 WHEN 'running' THEN 3 ELSE 4 END, s.created_at, s.id LIMIT 1) AS active_stage
FROM processing_jobs j
LEFT JOIN knowledges k ON k.id=j.knowledge_id AND k.tenant_id=j.tenant_id
LEFT JOIN data_sources ds ON ds.id=j.datasource_id AND ds.tenant_id=j.tenant_id
LEFT JOIN knowledge_bases kb ON kb.id=j.knowledge_base_id AND kb.tenant_id=j.tenant_id""")
    text += view('current_document_lifecycle', f"""SELECT j.*, j.job_id AS row_id, j.job_status AS status, COALESCE(j.active_stage,'') AS stage, j.updated_at AS observed_at,
 p.job_id AS published_job_id, p.generation AS published_generation,
 CASE WHEN j.is_published THEN 'current_available' WHEN p.job_id IS NOT NULL THEN 'previous_available' ELSE 'not_published' END AS availability,
 'verified_v2' AS evidence_basis
FROM mwe_processing_job_state j
LEFT JOIN mwe_processing_job_state p ON p.tenant_id=j.tenant_id AND p.knowledge_base_id=j.knowledge_base_id
 AND p.datasource_id=j.datasource_id AND p.external_id=j.external_id AND p.kind='document' AND p.is_published
WHERE j.kind='document' AND (j.is_current OR (j.is_published AND NOT EXISTS
 (SELECT 1 FROM processing_jobs n WHERE n.tenant_id=j.tenant_id AND n.knowledge_base_id=j.knowledge_base_id
 AND n.datasource_id=j.datasource_id AND n.external_id=j.external_id AND n.kind='document' AND n.is_current)))
UNION ALL
SELECT '' AS job_id, k.tenant_id, k.knowledge_base_id, {js('k.metadata','datasource_id')},
 COALESCE({js('k.metadata','external_id')},''), '' AS run_id, k.id AS knowledge_id,
 'document' AS kind, NULL AS generation, '' AS source_revision, '' AS configuration_revision, '' AS pipeline_fingerprint,
 NULL AS revision, FALSE AS is_current, FALSE AS is_published, NULL AS publication_epoch, FALSE AS rollback_pin,
 'legacy_unverified' AS retirement_state, 'unknown' AS readiness, 'unknown' AS completeness, k.parse_status, FALSE AS plan_sealed,
 k.title, '' AS folder_path, COALESCE(ds.name,''), COALESCE(kb.name,''),
 k.created_at, k.updated_at, NULL AS published_at, NULL AS finished_at,
 NULL AS steps_total, NULL AS steps_succeeded, NULL AS steps_problem, NULL AS cleanup_problem,
 NULL AS next_retry_at, NULL AS heartbeat_at, NULL AS resources_active, NULL AS charged_bytes, NULL AS estimated_index_bytes,
 NULL AS doc_type, NULL AS native_raw_bytes, NULL AS native_pages, NULL AS native_units, NULL AS images, NULL AS media_bytes, NULL AS active_stage,
 'legacy:' || k.id AS row_id, k.parse_status AS status, '' AS stage, k.updated_at AS observed_at,
 NULL AS published_job_id, NULL AS published_generation,
 CASE WHEN k.enable_status='enabled' AND k.parse_status IN ('completed','finalizing') THEN 'legacy_available' ELSE 'legacy_unverified' END AS availability,
 'legacy_unverified' AS evidence_basis
FROM knowledges k
LEFT JOIN data_sources ds ON ds.id={js('k.metadata','datasource_id')} AND ds.tenant_id=k.tenant_id
LEFT JOIN knowledge_bases kb ON kb.id=k.knowledge_base_id AND kb.tenant_id=k.tenant_id
WHERE k.deleted_at IS NULL AND COALESCE({js('k.metadata','processing_protocol')},'')<>'2'
 AND COALESCE({js('k.metadata','datasource_id')},'')<>''""")
    counts = []
    for kind, label in [('document', 'documents'), ('container', 'containers'), ('link', 'external_links')]:
        counts.append(f"(SELECT COUNT(*) FROM sync_run_items i WHERE i.tenant_id=j.tenant_id AND i.run_id=j.run_id AND i.kind='{kind}') AS {label}")
    for status in ['succeeded', 'blocked', 'failed', 'canceled', 'superseded', 'skipped']:
        counts.append(f"""(SELECT COUNT(*) FROM sync_run_items i JOIN processing_jobs d ON d.id=i.job_id AND d.tenant_id=i.tenant_id
 WHERE i.tenant_id=j.tenant_id AND i.run_id=j.run_id AND i.kind='document' AND d.status='{status}') AS documents_{status}""")
    counts.append("""(SELECT COUNT(*) FROM sync_run_items i WHERE i.tenant_id=j.tenant_id AND i.run_id=j.run_id AND i.kind='document'
 AND COALESCE(i.job_id,'')='') AS documents_unadmitted""")
    counts.append("""(SELECT COUNT(*) FROM sync_run_items i JOIN processing_jobs d ON d.id=i.job_id AND d.tenant_id=i.tenant_id
 WHERE i.tenant_id=j.tenant_id AND i.run_id=j.run_id AND i.kind='document'
 AND d.status NOT IN ('succeeded','blocked','failed','canceled','superseded','skipped')) AS documents_active""")
    text += view('current_run_progress', f"""SELECT j.*, j.job_id AS row_id, j.job_status AS status, 'scan' AS stage, j.updated_at AS observed_at,
 {', '.join(counts)},
 (SELECT e.to_state FROM processing_events e WHERE e.tenant_id=j.tenant_id AND e.job_id=j.job_id AND e.event_type='run_finished' ORDER BY e.id LIMIT 1) AS original_finished_status,
 (SELECT e.created_at FROM processing_events e WHERE e.tenant_id=j.tenant_id AND e.job_id=j.job_id AND e.event_type='run_finished' ORDER BY e.id LIMIT 1) AS original_finished_at
FROM mwe_processing_job_state j WHERE j.kind='scan' AND
 (j.job_status NOT IN ('succeeded','canceled','superseded','skipped','failed') OR NOT EXISTS
 (SELECT 1 FROM processing_jobs n WHERE n.kind='scan' AND n.tenant_id=j.tenant_id
 AND n.datasource_id=j.datasource_id AND n.created_at>j.created_at))""")
    text += view('stage_retry_queue', """SELECT j.*, s.id AS row_id, s.id AS step_id, s.status, s.stage, s.phase, s.unit_key,
 s.step_attempt, s.dispatch_seq, s.retry_count, s.max_retries, s.next_run_at, s.deadline_at, s.lease_expires_at,
 s.required_for_ready, s.required_for_completion, s.error_class, s.error_code, s.last_error_event_id,
 s.updated_at AS observed_at
FROM mwe_processing_job_state j JOIN processing_steps s ON s.job_id=j.job_id
WHERE s.status IN ('enqueue_pending','queued','running','waiting_external','retry_wait','failed','blocked')""")
    text += view('attempt_timeline', """SELECT j.*, CAST(e.id AS TEXT) AS row_id, e.id AS event_id, e.step_id,
 COALESCE(s.stage,'') AS stage, e.event_type, e.to_state AS status, e.job_revision AS event_revision,
 e.step_attempt, e.dispatch_seq, e.from_state, e.error_class, e.error_code,
 e.actor, e.action, e.operation_request_id, e.queue_task_id, e.trace_id,
 e.resolves_event_id, e.resolution_type, e.created_at AS observed_at
FROM processing_events e JOIN mwe_processing_job_state j ON j.job_id=e.job_id AND j.tenant_id=e.tenant_id
LEFT JOIN processing_steps s ON s.id=e.step_id AND s.job_id=e.job_id""")
    incident_columns = ','.join('e.'+column for column in TIMELINE_COLUMNS) if legacy else 'e.*'
    text += view('unresolved_incidents', f"""SELECT {incident_columns} FROM mwe_processing_attempt_timeline e
WHERE COALESCE(e.error_code,'')<>'' AND NOT EXISTS
 (SELECT 1 FROM processing_events r WHERE r.tenant_id=e.tenant_id AND r.job_id=e.job_id
 AND r.resolves_event_id=e.event_id AND COALESCE(r.resolution_type,'')<>'')""" + (" AND e.kind<>'legacy'" if legacy else ''))
    anomalies = []
    def anomaly(code, predicate, join='', suffix="j.job_id", step="''", stage="''"):
        anomalies.append(f"""SELECT j.*, '{code}:' || {suffix} AS row_id, '{code}' AS status, {stage} AS stage,
 {step} AS step_id, j.updated_at AS observed_at FROM mwe_processing_job_state j {join} WHERE {predicate}""")
    anomaly('READY_NOT_PUBLISHED', f"j.is_current AND NOT j.is_published AND j.readiness='ready' AND {age('j.updated_at')}>300")
    anomaly('PLAN_UNSEALED', f"j.is_current AND NOT j.plan_sealed AND {age('j.created_at')}>300")
    sj = 'JOIN processing_steps s ON s.job_id=j.job_id'
    anomaly('LEASE_EXPIRED', f"s.status='running' AND {age('s.lease_expires_at')}>0", sj, 's.id', 's.id', 's.stage')
    anomaly('NO_PROGRESS', f"s.status='running' AND {age('s.lease_expires_at')}<0 AND {age('s.progress_at')}>300", sj, 's.id', 's.id', 's.stage')
    anomaly('RETRY_OVERDUE', f"s.status='retry_wait' AND {age('s.next_run_at')}>120", sj, 's.id', 's.id', 's.stage')
    anomaly('PROJECTION_NOT_CONVERGED', f"j.is_published AND s.phase='projection' AND s.stage<>'retire_previous' AND s.status NOT IN ('succeeded','skipped') AND {age('j.published_at')}>300", sj, 's.id', 's.id', 's.stage')
    anomaly('OUTBOX_OVERDUE', f"""s.status IN ('enqueue_pending','queued') AND o.delivered_at IS NULL AND o.op='deliver'
 AND {age('o.available_at')}>120""", sj + ' JOIN task_pending_ops o ON o.tenant_id=j.tenant_id AND o.step_id=s.id AND o.step_attempt=s.step_attempt AND o.dispatch_seq=s.dispatch_seq', 's.id', 's.id', 's.stage')
    anomaly('DEAD_LETTER_RUNNING', f"""s.status='running' AND EXISTS (SELECT 1 FROM task_dead_letters d
 WHERE d.tenant_id=j.tenant_id AND {js('d.payload','job_id')}=j.job_id AND {js('d.payload','step_id')}=s.id
 AND {js('d.payload','generation')}=CAST(j.generation AS TEXT)
 AND {js('d.payload','step_attempt')}=CAST(s.step_attempt AS TEXT) AND {js('d.payload','dispatch_seq')}=CAST(s.dispatch_seq AS TEXT))""", sj, 's.id', 's.id', 's.stage')
    anomaly('RESOURCE_MISSING', """j.retirement_state='retained' AND EXISTS (SELECT 1 FROM resource_bindings b
 LEFT JOIN resources r ON r.id=b.resource_id AND r.tenant_id=b.tenant_id
 WHERE b.tenant_id=j.tenant_id AND b.owner_type='processing_job' AND b.owner_id=j.job_id
 AND (r.id IS NULL OR r.state<>'active' OR r.deleted_at IS NOT NULL))""")
    anomaly('COMPATIBILITY_DRIFT', """j.is_published AND (k.id IS NULL OR k.deleted_at IS NOT NULL OR k.parse_status NOT IN ('completed','finalizing') OR (j.job_status='succeeded' AND k.parse_status<>'completed') OR k.enable_status<>'enabled')""",
            'LEFT JOIN knowledges k ON k.id=j.knowledge_id AND k.tenant_id=j.tenant_id')
    anomaly('RETIREMENT_BLOCKED', "s.phase='retire' AND s.status IN ('failed','blocked')", sj, 's.id', 's.id', 's.stage')
    anomaly('FAQ_GC_STUCK', f"w.state='deleting' AND {age('w.updated_at')}>300", 'JOIN faq_index_writes w ON w.job_id=j.job_id AND w.tenant_id=j.tenant_id', 'w.id')
    anomaly('RESOURCE_GC_STUCK', f"r.state='deleting' AND {age('r.updated_at')}>300", 'JOIN resources r ON r.creation_job_id=j.job_id AND r.tenant_id=j.tenant_id', 'r.id')
    text += view('lifecycle_inconsistencies', '\nUNION ALL\n'.join(anomalies))
    if not tables:
        return text
    timestamp = 'TIMESTAMPTZ' if dialect == 'versioned' else 'DATETIME'
    text += f"""CREATE TABLE processing_history_snapshots (
 id TEXT PRIMARY KEY, tenant_id BIGINT NOT NULL, principal TEXT NOT NULL, kb_scope TEXT NOT NULL,
 view_name TEXT NOT NULL, filter_digest TEXT NOT NULL, total INTEGER NOT NULL CHECK(total BETWEEN 0 AND 10000),
 created_at {timestamp} NOT NULL, expires_at {timestamp} NOT NULL
);
CREATE INDEX idx_processing_snapshot_principal ON processing_history_snapshots(tenant_id, principal, created_at);
CREATE INDEX idx_processing_snapshot_expiry ON processing_history_snapshots(expires_at);
CREATE TABLE processing_history_rows (
 snapshot_id TEXT NOT NULL REFERENCES processing_history_snapshots(id) ON DELETE CASCADE,
 rank INTEGER NOT NULL, payload TEXT NOT NULL, PRIMARY KEY(snapshot_id, rank)
);
"""
    return text


def main():
    for dialect, version in [('versioned', '000087'), ('sqlite', '000010')]:
        up = ROOT / 'migrations' / dialect / f'{version}_processing_lifecycle.up.sql'
        down = up.with_name(up.name.replace('.up.', '.down.'))
        outputs = {up: render(dialect), down: 'DROP TABLE processing_history_rows;\nDROP TABLE processing_history_snapshots;\n' + ''.join(
            f'DROP VIEW mwe_processing_{view};\n' for view in reversed(VIEWS))}
        latest = '000089' if dialect == 'versioned' else '000012'
        drop = ''.join(f'DROP VIEW mwe_processing_{view};\n' for view in reversed(VIEWS))
        outputs[up.with_name(latest+'_processing_lifecycle.up.sql')] = drop+render(dialect,legacy=True,tables=False)
        outputs[up.with_name(latest+'_processing_lifecycle.down.sql')] = drop+''.join(f'DROP VIEW mwe_processing_{view};\n' for view in reversed(LEGACY_VIEWS))+render(dialect,tables=False)
        for path, content in outputs.items():
            content = content.rstrip() + '\n'
            if '--check' in sys.argv:
                assert path.read_text(encoding='utf-8') == content, f'Stale generated SQL: {path}'
            else:
                path.write_text(content, encoding='utf-8')


if __name__ == '__main__':
    main()
