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
- Failure tables are unresolved queues, not time-window views: a document row
  leaves only when its current parse status is no longer failed; a data-source
  row leaves only after that source's latest sync completes successfully.
- Document failures, latest data-source failures and dead letters use separate
  top navigation page selectors with a hard SQL limit of 50 rows per page.
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
