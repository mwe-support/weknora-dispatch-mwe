# MWE knowledge-base observability dashboard v1

## Release target

- Upstream application version: WeKnora v0.7.2
- Target branch: `weknora-v0.7.2`
- Initial deployment host: `marvel-kb` (`root@192.168.18.25` from local SSH config)
- Deployment path: `/public/knowledgebase/observability`

## Change scope and motivation

Adds a containerized Grafana, Prometheus, Loki and Grafana Alloy stack plus a
small fixed-purpose capability probe. Existing Docker health checks only prove
that an HTTP endpoint responds; they do not prove that a GPU-backed component
can execute its required workload.

The dashboard also joins existing WeKnora task records so operators can locate
failed work by workspace/account, data source, space, document and processing
stage without exposing credentials or document bodies.

## User and deployment impact

- New containers use the `kb-observability-*` namespace. Grafana binds only to
  the server LAN address `192.168.18.25:13000`; Prometheus, Loki and Alloy stay
  on loopback ports 19090, 13100 and 12345.
- No existing WeKnora, model, database or storage service is restarted by the
  observability Compose project.
- The installer creates a dedicated PostgreSQL read-only role and stores its
  password with the Grafana admin and Redis monitoring credentials in a mode
  `0600` host-only file.
- Alloy and the capability probe access the Docker socket; this is a privileged
  monitoring boundary and the endpoints remain loopback-only.

## Validation

- JSON dashboard parse: passed.
- YAML parse across eight configuration files: passed.
- Capability-probe self-test: passed.
- Prometheus config validation: passed; one rule file and eight alert rules.
- Alloy v1.18.0 configuration validation: passed.
- Live deployment readiness: passed for Grafana, Prometheus, Loki and Alloy.
- Secret file: `0600 root:root`.
- Grafana provisioning: dashboard loaded; Prometheus, Loki and PostgreSQL
  datasources all returned `status=OK`.
- Metrics: MinerU HTTP probe `1` while in-container CUDA capability `0`, proving
  the dashboard does not collapse transport and workload health.
- Loki: production Docker streams received with container, host, service and
  stack labels.
- Detailed queries using `weknora_observer`: passed. Six-hour counts at
  validation time were 100 document-processing failures, 14 data-source sync
  failures/partial runs and 323 dead letters; no business record content was
  printed during this check.
- Self-observability: zero error/fatal/panic/traceback matches across all five
  monitoring containers in the final 60-second verification window.
- Existing core services: App, PostgreSQL, Redis, MinerU and Qdrant remained
  running; existing health states were unchanged.
- LAN exposure: server interface `eno1np0` owns `192.168.18.25/24`; Grafana
  direct access on `192.168.18.25:13000` passed while backend telemetry ports
  remained loopback-only. UFW was inactive at validation time.
- Final firing alerts: only `MinerUCUDANotReady`. A stale MCP `/health` series
  was removed from alert/dashboard evaluation by an explicit component
  allowlist; the current `mcp-transport` TCP probe reports success.

## Production evidence collected before deployment

- Live application version: `0.7.2`, branch
  `production/default-vlm-20260810`, commit `70445bf3`.
- `kb-mineru-api` Docker state: healthy.
- MinerU container capability: Torch `2.11.0+cu130`, CUDA unavailable, zero
  devices; `nvidia-smi` failed to initialize NVML.
- Knowledge state snapshot: completed 2,399; draft 13; failed 33;
  finalizing 280; processing 222.
- Failed knowledge updated in the previous 24 hours: 97.
- Knowledge metadata coverage: `space_id` 5,148; `space_name` 0.
- Obsolete stopped `kb-cloudflared` container removed on explicit user request;
  no image, network, volume or configuration was deleted.

## Rollback

From `/public/knowledgebase/observability`:

```bash
docker compose --env-file /public/knowledgebase/secrets/observability.env \
  --profile backend --profile agent stop
```

This stops only `kb-observability-*` containers and preserves their named
volumes. Removing the read-only database role or monitoring data requires a
separate explicit decision.

## Residual risks

- Docker socket access is root-equivalent.
- Log signatures cover known blocking error classes and require tuning from
  observed production false positives/negatives.
- HTTP probes still require later replacement with real model/parse canaries.
- Current records do not preserve source `space_name` or a safe connector
  account snapshot.
- v1 detailed task tables require Grafana to reach PostgreSQL locally; remote
  backend migration needs a narrow safe event bridge.
