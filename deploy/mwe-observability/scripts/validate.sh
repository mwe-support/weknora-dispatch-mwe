#!/usr/bin/env bash
set -Eeuo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
python3 -m json.tool "$ROOT/grafana/dashboards/knowledgebase-overview.json" >/dev/null
PROBE_SELF_TEST=1 EXPECTED_CONTAINERS=test MINERU_CONTAINER=test \
  python3 "$ROOT/capability-probe/probe.py"

if command -v docker >/dev/null 2>&1; then
  docker run --rm --entrypoint promtool \
    -v "$ROOT/prometheus:/etc/prometheus:ro" \
    prom/prometheus:v3.11.3 check config /etc/prometheus/prometheus.yml
  docker run --rm \
    -e MONITORED_HOST=test \
    -e PROMETHEUS_REMOTE_WRITE_URL=http://127.0.0.1:9090/api/v1/write \
    -e LOKI_WRITE_URL=http://127.0.0.1:3100/loki/api/v1/push \
    -e WEKNORA_DB_HOST=postgres \
    -e WEKNORA_DB_PORT=5432 \
    -e WEKNORA_DB_NAME=weknora \
    -e WEKNORA_DB_USER=observer \
    -e WEKNORA_DB_PASSWORD=test \
    -e WEKNORA_REDIS_HOST=redis \
    -e WEKNORA_REDIS_PORT=6379 \
    -e WEKNORA_REDIS_PASSWORD=test \
    -v "$ROOT/alloy:/etc/alloy:ro" \
    grafana/alloy:v1.18.0 validate /etc/alloy/config.alloy
fi

echo "VALIDATION=passed"
