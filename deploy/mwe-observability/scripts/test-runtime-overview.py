"""Check that the overview separates executing, queued, waiting and abnormal stages."""
import argparse
import json
from pathlib import Path
import re
import subprocess

parser = argparse.ArgumentParser()
parser.add_argument('--dashboard', type=Path, default=Path(__file__).resolve().parents[1] / 'grafana/dashboards/knowledgebase-overview.json')
parser.add_argument('--postgres-container')
parser.add_argument('--prometheus-container')
parser.add_argument('--observer-sql', type=Path, default=Path(__file__).resolve().parents[1] / 'grafana/queries/observer-access.sql')
args = parser.parse_args()
dashboard = json.loads(args.dashboard.read_text(encoding='utf-8'))
panels = {p['id']: p for p in dashboard['panels']}
expected = {104: 1, 110: 2, 111: 2, 103: 4, 102: 1, 101: 1}
assert set(expected) <= panels.keys(), 'Overview mixes execution with backlog: separate stage cards are missing'
assert {112, 113} <= panels.keys(), 'Actual model requests and provider queues must be visible'
for panel_id in expected:
    query = panels[panel_id]['targets'][0]['rawSql']
    assert 'FROM processing_runtime_counts' in query, 'Overview counts must avoid detailed evidence joins'
for panel_id in (112, 113):
    expr = panels[panel_id]['targets'][0]['expr']
    assert 'llamacpp:' in expr and 'vllm:' in expr and 'up{' in expr
    assert 'or vector(0)' not in expr, 'Missing model telemetry must not look idle'
    assert panels[panel_id]['fieldConfig']['defaults']['noValue'] == '采集不完整'

if args.postgres_container:
    # Executes each real panel query against one small read-only SQL relation.
    # Includes a previous generation, a legacy row and missing/expired leases.
    definition = re.search(r'CREATE OR REPLACE VIEW :"observer_schema"\.processing_runtime_counts AS\s*(.*?);', args.observer_sql.read_text(encoding='utf-8'), re.S).group(1).replace(':"app_schema".', '')
    fixture = """WITH processing_jobs(id,kind,is_current) AS (VALUES
      (1,'document',true),(2,'document',false),(3,'legacy',true),(4,'scan',true)),
      processing_steps(job_id,status,lease_expires_at) AS (VALUES
      (1,'running',now()+interval '1 minute'),
      (1,'running',now()-interval '1 minute'),
      (1,'running',NULL::timestamptz),
      (1,'queued',NULL::timestamptz),
      (1,'enqueue_pending',NULL::timestamptz),
      (1,'waiting_external',NULL::timestamptz),
      (1,'retry_wait',NULL::timestamptz),
      (1,'failed',NULL::timestamptz),
      (1,'blocked',NULL::timestamptz),
      (2,'running',now()+interval '1 minute'),
      (3,'running',now()+interval '1 minute'),
      (1,'planned',NULL::timestamptz)), processing_runtime_counts AS (""" + definition + ') '
    for panel_id, count in expected.items():
        sql = "BEGIN READ ONLY; SET LOCAL statement_timeout='3s'; " + fixture + panels[panel_id]['targets'][0]['rawSql'] + '; COMMIT;'
        result = subprocess.check_output(['docker', 'exec', args.postgres_container, 'psql', '-X', '-qAt', '-v', 'ON_ERROR_STOP=1', '-U', 'weknora', '-d', 'weknora', '-c', sql], text=True, timeout=8)
        assert int(result.strip()) == count, (panel_id, result, count)
    print('PASS: real panel SQL separates active leases, backlog, external wait, failed and expired stages')
else:
    print('PASS: overview has separate stage and model telemetry; use --postgres-container for SQL regression')

if args.prometheus_container:
    targets = [('q4-gpu0','llamacpp:requests_processing','llamacpp:requests_deferred'),
               ('q4-gpu1','llamacpp:requests_processing','llamacpp:requests_deferred'),
               ('embedding','vllm:num_requests_running','vllm:num_requests_waiting'),
               ('reranker','vllm:num_requests_running','vllm:num_requests_waiting')]
    tests = []
    for case in ('idle', 'busy', 'missing_metric', 'down'):
        series = []
        for index, (job, running, waiting) in enumerate(targets):
            series.append({'series':f'up{{job="{job}"}}','values':('0' if case=='down' and index==3 else '1')+'+0x4'})
            if case=='missing_metric' and index==3:
                continue
            for metric in (running, waiting):
                series.append({'series':f'{metric}{{job="{job}"}}','values':str(index+1 if case=='busy' else 0)+'+0x4'})
        samples = [] if case in ('missing_metric', 'down') else [{'labels':'{}','value':10 if case=='busy' else 0}]
        tests.append({'interval':'15s','input_series':series,'promql_expr_test':[
            {'expr':panels[panel_id]['targets'][0]['expr'],'eval_time':'1m','exp_samples':samples} for panel_id in (112, 113)]})
    subprocess.run(['docker','exec','-i',args.prometheus_container,'promtool','test','rules','/dev/stdin'],input=json.dumps({'evaluation_interval':'15s','tests':tests}),text=True,timeout=20,check=True)
    print('PASS: real model queries distinguish idle, busy, missing telemetry and down targets')
