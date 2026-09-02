# Observability GPU metrics and unresolved failure queues

## Release target

- WeKnora base: v0.7.2
- Target branch: `weknora-v0.7.2`
- Deployment host: `marvel-kb` (`192.168.18.25`)
- Project directory: `/public/knowledgebase/observability`

## Change scope and motivation

- Extend the existing fixed-purpose capability probe with per-GPU utilization,
  framebuffer usage, temperature, board power and power-limit metrics.
- Reuse `nvidia-smi` inside the existing `kb-q4-gpu0` and `kb-q4-gpu1`
  containers. The Alpine probe image cannot execute the host-injected glibc
  `nvidia-smi`, so no new GPU exporter or runtime dependency was added.
- Add four live GPU cards and two GPU trend panels.
- Replace the combined server-resource panel with separate CPU, memory,
  network-throughput and disk-I/O trends.
- Convert document and data-source failure tables from time-window views to
  unresolved queues.

## Resolution semantics

- A document failure remains visible while its current knowledge row has
  `parse_status='failed'`; it disappears only after the current state changes.
- A data-source failure remains visible while that data source's latest sync is
  `failed` or `partial`; a later successful sync removes it.
- `sync_logs.result.errors` currently contains only `title`, `code` and
  `message`, not a stable `external_id` or `knowledge_id`. The dashboard does
  not guess document identity by title.

## Deployment impact

- Rebuilt only `kb-observability-capability-probe` and restarted only
  `kb-observability-alloy` to refresh Docker discovery after the probe
  container ID changed.
- Prometheus rules were hot reloaded.
- Grafana picked up the provisioned dashboard file automatically.
- No WeKnora, database, Redis, MinerU, Qdrant or model container was restarted.

## Validation

- Local dashboard JSON, YAML and probe self-test: passed.
- Prometheus configuration: passed; nine alert rules.
- Both GPU probe readiness series: `1`.
- Live GPU snapshot during validation:
  - GPU 0: 89% utilization, 44.3% framebuffer, 79 C, approximately 300 W.
  - GPU 1: 91% utilization, 84.4% framebuffer, 79 C, approximately 302 W.
- CPU, I/O wait, memory, Swap, network receive/transmit and per-device disk
  read/write PromQL expressions all returned successful vectors.
- Browser QA in the user's authenticated Chrome session:
  - Page identity and dashboard title matched.
  - Page was non-blank with no framework error overlay.
  - Manual Refresh populated every new GPU card and trend.
  - CPU, memory, network and disk-I/O panels rendered independently.
  - Unresolved document failure table rendered current records outside the
    selected six-hour time range semantics.
  - Table status coloring was corrected so only state values are colored;
    component names and host labels remain neutral.
- Two non-blocking Grafana frontend warnings remained: a Moment date-format
  deprecation and a fully-loaded folder pagination warning.
- Final Alloy and capability-probe error counts: zero.

## Rollback

Restore the previous repository versions of:

- `deploy/mwe-observability/capability-probe/probe.py`
- `deploy/mwe-observability/grafana/dashboards/knowledgebase-overview.json`
- `deploy/mwe-observability/prometheus/rules/knowledgebase.yml`

Then rebuild only `capability-probe`, hot reload Prometheus and restart Alloy
to refresh the recreated probe container ID.

## Residual risks

- GPU telemetry depends on the two Q4 containers being available; the
  `KnowledgeBaseGPUProbeUnavailable` alert distinguishes missing telemetry.
- GPU history begins with this deployment; there is no retroactive series.
- Per-document data-source error resolution needs future persistence of a
  stable source document ID in `SyncItemError`.
- Current knowledge metadata stores `space_id` but not `space_name`.
