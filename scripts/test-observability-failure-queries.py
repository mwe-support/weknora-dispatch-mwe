"""Emit synthetic transactional PostgreSQL regression tests; never reads secrets."""
import argparse
import importlib.util
import json
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location("renderer", Path(__file__).with_name("render-observability-failure-queries.py"))
renderer = importlib.util.module_from_spec(spec)
spec.loader.exec_module(renderer)

FIXTURE = r"""
\set ON_ERROR_STOP on
BEGIN;
SET LOCAL TIME ZONE 'UTC';
CREATE TEMP TABLE tenants(id bigint, name text);
CREATE TEMP TABLE knowledge_bases(id text,tenant_id bigint,name text,deleted_at timestamptz);
CREATE TEMP TABLE data_sources(id text,tenant_id bigint,knowledge_base_id text,name text,type text,status text,deleted_at timestamptz);
CREATE TEMP TABLE sync_logs(id text,data_source_id text,tenant_id bigint,status text,started_at timestamp,finished_at timestamp,updated_at timestamp,result jsonb,items_total int,items_failed int,error_message text);
CREATE TEMP TABLE knowledges(id text,tenant_id bigint,knowledge_base_id text,title text,file_name text,file_size bigint,metadata jsonb,parse_status text,channel text,created_at timestamptz,updated_at timestamptz,deleted_at timestamptz,error_message text);
CREATE TEMP TABLE knowledge_processing_spans(knowledge_id text,name text,error_code text,error_message text,status text,finished_at timestamptz,updated_at timestamptz);
INSERT INTO tenants VALUES (1,'workspace'),(2,'other');
INSERT INTO knowledge_bases VALUES ('kb',1,'knowledge base',NULL),('removed-kb',1,'removed',now());
INSERT INTO data_sources VALUES ('ds',1,'kb','source','tencent_docs','active',NULL),('deleted',1,'kb','deleted source','tencent_docs','active',now()),('kb-deleted',1,'removed-kb','removed KB source','tencent_docs','active',NULL);
INSERT INTO sync_logs(id,data_source_id,tenant_id,status,started_at,updated_at,result) VALUES
('first','ds',1,'partial','2026-01-01','2026-01-01',jsonb_build_object('errors',jsonb_build_array(
 jsonb_build_object('title','no knowledge','file_id','missing','stage','download','category','FILE_SIZE_EXCEEDED','limit_bytes',104857600,'actual_bytes',1457777536,'message','文件大小超出上限'),
 jsonb_build_object('title','old completed','file_id','old','message','export failed'),
 jsonb_build_object('title','recovered','file_id','recovered','message','export failed'),
 jsonb_build_object('title','still processing','file_id','processing','message','export failed'),
 jsonb_build_object('title','legacy title','message','download Tencent Docs export exceeds 104857600 bytes'),
 jsonb_build_object('title','code111','file_id','code111','message','manage.export_progress Service Error (code=111)'),
 jsonb_build_object('title','combined','file_id','combined','message','download failed'),
 jsonb_build_object('title','alias','external_id','external-alias','file_id','alias','message','old alias error'),
 jsonb_build_object('title','deleted knowledge','file_id','deleted-k','message','download failed'),
 jsonb_build_object('title','stream too large','file_id','stream','category','FILE_SIZE_EXCEEDED','limit_bytes',8,'observed_at_least_bytes',9,'message','文件大小超出上限'),
 jsonb_build_object('title','bad timestamp','file_id','bad-time','occurred_at','not a timestamp','message','failed')))),
('second','ds',1,'partial','2026-01-02','2026-01-02','{"errors":[{"title":"no knowledge","file_id":"missing","stage":"download","category":"FILE_SIZE_EXCEEDED","limit_bytes":104857600,"actual_bytes":1457777536,"message":"latest size failure"},{"title":"legacy title","message":"download Tencent Docs export exceeds 104857600 bytes"},{"title":"alias","external_id":"external-alias","message":"latest alias error"}]}'),
('incremental-no-fetch','ds',1,'success','2026-01-04','2026-01-04','{"errors":[]}'),
('deleted-source-error','deleted',1,'failed','2026-01-03','2026-01-03','{"errors":[{"title":"must hide","file_id":"hidden"}]}'),
('deleted-kb-error','kb-deleted',1,'failed','2026-01-03','2026-01-03','{"errors":[{"title":"must hide","file_id":"hidden-kb"}]}'),
('malformed-result','ds',1,'success','2026-01-04','2026-01-04','{"errors":{}}'),
('tenant-mismatch','ds',2,'failed','2026-01-04','2026-01-04','{"errors":[{"title":"wrong tenant","file_id":"wrong-tenant"}]}');
INSERT INTO knowledges(id,tenant_id,knowledge_base_id,title,metadata,parse_status,created_at,updated_at) VALUES
('old',1,'kb','old completed','{"datasource_id":"ds","file_id":"old"}','completed','2025-12-01','2026-01-05'),
('recovered',1,'kb','recovered','{"datasource_id":"ds","file_id":"recovered"}','completed','2026-01-03','2026-01-05'),
('processing',1,'kb','still processing','{"datasource_id":"ds","file_id":"processing"}','finalizing','2026-01-03','2026-01-05'),
('legacy',1,'kb','legacy title','{"datasource_id":"ds","file_id":"do-not-infer-from-title"}','completed','2026-01-03','2026-01-05'),
('combined',1,'kb','combined','{"datasource_id":"ds","file_id":"combined"}','failed','2026-01-03','2026-01-05'),
('deleted-k',1,'kb','deleted knowledge','{"datasource_id":"ds","file_id":"deleted-k"}','completed','2026-01-03','2026-01-05'),
('hidden-parse',1,'kb','deleted-source parse','{"datasource_id":"deleted","file_id":"hidden-parse"}','failed','2026-01-03','2026-01-05');
UPDATE knowledges SET deleted_at=now() WHERE id='deleted-k';
INSERT INTO sync_logs(id,data_source_id,tenant_id,status,started_at,updated_at,result) VALUES
('more-cases','ds',1,'partial','2026-01-02','2026-01-02','{"errors":[{"title":"same title","file_id":"same-title-a"},{"title":"same title","file_id":"same-title-b"},{"title":"marker recovery","file_id":"marker"},{"title":"conflicting id","file_id":"correct-file","external_id":"conflict-external"}]}');
INSERT INTO knowledges(id,tenant_id,knowledge_base_id,title,metadata,parse_status,created_at,updated_at) VALUES
('marker',1,'kb','marker recovery','{"datasource_id":"ds","file_id":"marker","source_fetch_completed_at":"2026-01-03T00:00:00Z"}','completed','2025-12-01','2026-01-05'),
('conflicting',1,'kb','conflicting id','{"datasource_id":"ds","file_id":"wrong-file","external_id":"conflict-external"}','completed','2026-01-03','2026-01-05');
-- More than one page, plus matching parse/sync failure must deduplicate.
INSERT INTO knowledges(id,tenant_id,knowledge_base_id,title,metadata,parse_status,created_at,updated_at)
SELECT 'page-'||i,1,'kb','synthetic '||i,'{}','failed','2026-01-03','2026-01-05' FROM generate_series(1,55) i;
UPDATE knowledges SET file_size=CASE id WHEN 'page-1' THEN 1024 WHEN 'page-2' THEN 1048576 WHEN 'page-3' THEN 1073741824 ELSE 2048 END;
UPDATE sync_logs SET result=jsonb_set(result,'{errors,0,source_path}','"源空间/目录/文档"'::jsonb) WHERE id='second';
"""

ASSERTIONS = r"""
DO $$ BEGIN
 IF (SELECT count(*) FROM actual WHERE file_key='file:missing')<>1 THEN RAISE EXCEPTION 'missing knowledge or dedup failed'; END IF;
 IF (SELECT sync_log_id FROM actual WHERE file_key='file:missing')<>'second' THEN RAISE EXCEPTION 'latest per-file event lost'; END IF;
 IF NOT EXISTS (SELECT 1 FROM actual WHERE file_key='file:old') THEN RAISE EXCEPTION 'old completed or unrelated success falsely cleared failure'; END IF;
 IF EXISTS (SELECT 1 FROM actual WHERE file_key='file:recovered') THEN RAISE EXCEPTION 'new completed version did not resolve'; END IF;
 IF NOT EXISTS (SELECT 1 FROM actual WHERE file_key='file:processing') THEN RAISE EXCEPTION 'finalizing resolved too early'; END IF;
 IF (SELECT count(*) FROM actual WHERE identity_basis='legacy_title_only')<>1 THEN RAISE EXCEPTION 'legacy dedup/recovery changed'; END IF;
 IF EXISTS (SELECT 1 FROM actual WHERE data_source_id IN ('deleted','kb-deleted') OR file_key='file:wrong-tenant') THEN RAISE EXCEPTION 'deleted source or tenant mismatch leaked'; END IF;
 IF (SELECT count(*) FROM actual WHERE file_key='file:combined')<>1 OR (SELECT linked_sync_log_id FROM actual WHERE file_key='file:combined')<>'first' THEN RAISE EXCEPTION 'parse/fetch correlation failed'; END IF;
 IF (SELECT count(*) FROM actual WHERE file_key='file:alias')<>1 THEN RAISE EXCEPTION 'stable alias dedup failed'; END IF;
 IF (SELECT category FROM actual WHERE file_key='file:code111')='UNSUPPORTED_FILE_TYPE' THEN RAISE EXCEPTION '111 classified unsupported'; END IF;
 IF (SELECT category FROM actual WHERE file_key='legacy-title:legacy title')<>'FILE_SIZE_EXCEEDED' OR (SELECT actual_bytes FROM actual WHERE file_key='legacy-title:legacy title') IS NOT NULL THEN RAISE EXCEPTION 'legacy size classification fabricated size'; END IF;
 IF (SELECT actual_bytes FROM actual WHERE file_key='file:stream') IS NOT NULL OR (SELECT observed_at_least_bytes FROM actual WHERE file_key='file:stream')<>'9' THEN RAISE EXCEPTION 'stream size fabricated'; END IF;
 IF NOT EXISTS (SELECT 1 FROM actual WHERE file_key='file:deleted-k') THEN RAISE EXCEPTION 'deleted knowledge falsely resolved'; END IF;
 IF EXISTS (SELECT 1 FROM actual WHERE file_key='file:marker') THEN RAISE EXCEPTION 'explicit newer source fetch marker did not resolve'; END IF;
 IF (SELECT count(*) FROM actual WHERE title='same title')<>2 THEN RAISE EXCEPTION 'distinct file IDs merged by title'; END IF;
 IF NOT EXISTS (SELECT 1 FROM actual WHERE file_key='file:correct-file') THEN RAISE EXCEPTION 'conflicting external identity falsely resolved'; END IF;
 IF (SELECT count(*) FROM detail_page)<>50 THEN RAISE EXCEPTION 'page size is not 50'; END IF;
 IF (SELECT __value::int FROM page_count)<>CEIL((SELECT count(*) FROM actual)/50.0)::int THEN RAISE EXCEPTION 'count/detail mismatch'; END IF;
 IF (SELECT count(*) FROM source_actual)<>0 THEN RAISE EXCEPTION 'deleted sources leaked into latest summary'; END IF;
 IF (SELECT "导入文件大小" FROM presentation_all WHERE "文档名称"='synthetic 1')<>'1.00 KB' THEN RAISE EXCEPTION 'KB display failed'; END IF;
 IF (SELECT "导入文件大小" FROM presentation_all WHERE "文档名称"='synthetic 2')<>'1.00 MB' THEN RAISE EXCEPTION 'MB display failed'; END IF;
 IF (SELECT "导入文件大小" FROM presentation_all WHERE "文档名称"='synthetic 3')<>'1.00 GB' THEN RAISE EXCEPTION 'GB display failed'; END IF;
 IF (SELECT "导入文件大小" FROM presentation_all WHERE "文档名称"='no knowledge')<>'1.36 GB' THEN RAISE EXCEPTION 'export size display failed'; END IF;
 IF (SELECT "导入文件大小" FROM presentation_all WHERE "文档名称"='stream too large')<>'未记录' THEN RAISE EXCEPTION 'stream prefix displayed as full size'; END IF;
 IF (SELECT "导入文件大小" FROM presentation_all WHERE "文档名称"='old completed')<>'未记录' THEN RAISE EXCEPTION 'old version size substituted'; END IF;
 IF (SELECT "详情"::jsonb->>'sync_log_id' FROM presentation_all WHERE "文档名称"='no knowledge')<>'second' THEN RAISE EXCEPTION 'details lost sync ID'; END IF;
 IF (SELECT count(*) FROM information_schema.columns WHERE table_name='presentation_all')<>11 THEN RAISE EXCEPTION 'main view must have 11 columns'; END IF;
 IF (SELECT "腾讯文档路径" FROM presentation_all WHERE "文档名称"='no knowledge')<>'源空间/目录/文档' THEN RAISE EXCEPTION 'recorded Tencent source path lost'; END IF;
 IF (SELECT "腾讯文档路径" FROM presentation_all WHERE "文档名称"='legacy title')<>'未记录' THEN RAISE EXCEPTION 'missing source path guessed'; END IF;
END $$;
SELECT 'PASS: identity, history, recovery, soft deletion, size classification, KB/MB/GB display, details, paging/count' AS result;
ROLLBACK;
"""


def build_sql():
    q = renderer.queries()
    doc_cte = (renderer.GRAFANA / "queries/document_failures.sql").read_text(encoding="utf-8")
    src_cte = (renderer.GRAFANA / "queries/source_failures.sql").read_text(encoding="utf-8")
    dashboard = json.loads(renderer.DASHBOARD.read_text(encoding="utf-8"))
    baseline = json.loads(subprocess.check_output([
        "git", "show", "HEAD:deploy/mwe-observability/grafana/dashboards/knowledgebase-overview.json"
    ], cwd=ROOT))
    # This patch must not restore old pager scripts, dead-letter filters, GPUs,
    # container navigation or any other already verified production UI.
    unchanged = json.loads(json.dumps(dashboard))
    for old in baseline["panels"]:
        if old["id"] in (10, 11):
            panel = next(p for p in unchanged["panels"] if p["id"] == old["id"])
            panel["description"] = old["description"]
            panel["targets"][0]["rawSql"] = old["targets"][0]["rawSql"]
            if old["id"] == 10:
                panel["fieldConfig"], panel["options"] = old["fieldConfig"], old["options"]
    for old in baseline["templating"]["list"]:
        if old["name"] in ("doc_pages", "sync_pages"):
            var = next(v for v in unchanged["templating"]["list"] if v["name"] == old["name"])
            var["query"], var["definition"] = old["query"], old["definition"]
    unchanged["version"] = baseline["version"]
    assert unchanged == baseline, "unrelated dashboard UI or query changed"
    for pid, (detail, count, var) in q.items():
        assert next(p for p in dashboard["panels"] if p["id"] == pid)["targets"][0]["rawSql"] == detail
        v = next(v for v in dashboard["templating"]["list"] if v["name"] == var)
        assert v["definition"] == v["query"] == count
    return (FIXTURE + "\nCREATE TEMP VIEW actual AS " + doc_cte + " SELECT * FROM failure_rows;\n"
            + "CREATE TEMP VIEW source_actual AS " + src_cte + " SELECT * FROM failure_rows;\n"
            + "CREATE TEMP VIEW detail_page AS " + q[10][0].replace("${doc_page:sqlstring}", "'1'") + ";\n"
            + "CREATE TEMP VIEW presentation_all AS " + q[10][0].rsplit(" LIMIT 50 OFFSET", 1)[0] + ";\n"
            + "CREATE TEMP VIEW page_count AS " + q[10][1] + ";\n" + ASSERTIONS)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--emit", type=Path, required=True)
    args = parser.parse_args()
    args.emit.write_text(build_sql(), encoding="utf-8")
    print("generated PostgreSQL fixture and verified dashboard/count consistency")
