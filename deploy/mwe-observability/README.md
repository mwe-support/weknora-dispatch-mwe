# MWE knowledge-base observability dashboard v1

First containerized operations dashboard for the `marvel-kb` WeKnora v0.7.2
deployment. It deliberately treats Docker health as only one signal.

## What v1 monitors

- Host and Docker CPU, memory, disk, I/O, restarts and OOM state.
- Per-GPU utilization, framebuffer usage, temperature, board power and trends,
  sampled from the two existing Q4 GPU containers with `nvidia-smi`.
- HTTP reachability for WeKnora, MinIO, Qdrant, MinerU, embedding, reranker
  and both Q4 workers; TCP transport reachability for MCP, which has no
  unauthenticated `/health` endpoint in the current deployment.
- PostgreSQL, Redis and current WeKnora task-state metrics.
- Docker logs in Loki, with common credential fields redacted before storage.
  Entries older than the seven-day retention window are dropped by Alloy
  before upload, avoiding noisy first-start backfill failures.
- A fixed-purpose Docker probe that checks expected containers, scans only the
  recent log window for blocking signatures, and executes
  `torch.cuda.is_available()` inside the running MinerU container.
- Grafana tables for failed documents, data-source sync errors and dead letters.
  The document table resolves workspace/account, data source, space ID,
  document, stage and error from existing WeKnora records.
- The document failure table also expands Tencent Docs per-file errors from
  `sync_logs.result.errors`, including fetch/export/download failures that have
  no knowledge row. It excludes deleted sources/KBs and has no time-window filter.
- A stable-ID source failure resolves only after a newer source read/re-ingestion
  has a current completed knowledge version. Old completed copies and unrelated
  incremental success do not resolve it. Legacy title-only errors remain marked
  as recovery-unverified; titles are not used to invent IDs or prove recovery.
- The data-source summary shows each active source's latest failed/partial sync;
  per-file unresolved evidence remains in the document view even if a later
  incremental run succeeds without fetching that file.
- The document table labels the destination explicitly as WeKnora workspace/KB.
  Import size uses the current knowledge file_size or that failure's recorded
  export size, formatted as KB/MB/GB (1024-based); unknown sizes say 未记录.
  Source-space identifiers, raw byte evidence and technical IDs live in the
  详情 cell inspector rather than separate main-table columns.
  腾讯文档路径 reads only recorded source_path metadata; existing records did
  not capture this field and show 未记录 until reliable source paths are stored.
- Shared SQL definitions in `grafana/queries/` generate both detail and page-count
  queries via `scripts/render-observability-failure-queries.py` in the repository.
  Error evidence is limited to retained sync logs and their 100-item error sample.
- Dead letters remain as a permanent audit archive in PostgreSQL, while the
  dashboard shows only the newest dead letter for each business object whose
  current document or data-source state is still unresolved.
- Document failures, latest data-source failures and dead letters use hidden
  server-side page variables with 50-row SQL limits. Each table has its own
  bottom pager with previous/next controls and an Enter-to-jump page input.
- The pager input uses the signed Grafana Labs Business Text panel pinned at
  `marcusolsson-dynamictext-panel@6.3.0` in Compose.
- The related-error log panel reads one Loki container stream at a time; use
  the horizontal container navigation bar directly above the panel to switch.

The capability probe mounts the Docker socket. Docker API access is
root-equivalent even with a read-only mount; keep the stack loopback-only and
do not install unreviewed Grafana plugins or Alloy configuration.

## Initial deployment on marvel-kb

Copy this directory to `/public/knowledgebase/observability`, then run:

```bash
cd /public/knowledgebase/observability
chmod +x scripts/*.sh
./scripts/validate.sh
./scripts/install.sh
```

`install.sh` creates `/public/knowledgebase/secrets/observability.env` with
mode `0600`, creates/rotates a PostgreSQL read-only role, validates both
external Docker networks, and starts the `backend` and `agent` profiles.

Access Grafana directly from the `192.168.18.0/24` LAN:

Open `http://192.168.18.25:13000`. Anonymous access and self-registration stay
disabled. The generated admin password remains only in
`/public/knowledgebase/secrets/observability.env`.

Prometheus, Loki and Alloy remain bound to `127.0.0.1`; only Grafana is exposed
on the LAN address.

The dashboard URL after login is
`/d/mwe-kb-observability-v1/088cff1` under the `MWE Knowledge Base` folder.

## Current evidence boundary

On 2026-09-01 the live `kb-mineru-api` container reported Docker `healthy`,
while its in-container checks returned `torch.cuda.is_available() == false`,
zero CUDA devices and an NVML initialization error. The v1 dashboard therefore
shows MinerU as failed even while its HTTP health endpoint is green.

The live database had 5,148 knowledge rows with `space_id` and zero with
`space_name`. v1 shows the space ID. Persisting the source space name during
connector ingestion is a follow-up application change.

## Remote backend later

Alloy already sends metrics through Prometheus remote write and logs through
the Loki push API. To move storage and visualization to
`marvel-local-server`, keep the `agent` profile and capability probe on
`marvel-kb`, protect remote-write/push with TLS and authentication, and point
`PROMETHEUS_REMOTE_WRITE_URL` / `LOKI_WRITE_URL` at the remote backend.

The current detailed task tables query PostgreSQL directly and are intentionally
single-host in v1. Do not expose PostgreSQL remotely. Before moving Grafana,
add a narrow task-event bridge that ships only the safe account/data-source/
space/document/error projection to the central backend.

## Intentionally deferred

- Notification receivers and escalation routing; no destination was selected.
- A real one-page MinerU parse canary; v1 checks CUDA inside the real container
  and uses log/endpoint evidence.
- `space_name` and connector account snapshot persistence.
- A central task-event bridge for remote Grafana deployment.
