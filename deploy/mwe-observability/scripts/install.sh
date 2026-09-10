#!/usr/bin/env bash
set -Eeuo pipefail

ROOT="${OBSERVABILITY_ROOT:-/public/knowledgebase/observability}"
SECRETS_FILE="${OBSERVABILITY_SECRETS_FILE:-/public/knowledgebase/secrets/observability.env}"
WEKNORA_ENV="${WEKNORA_ENV:-/public/knowledgebase/weknora/src/.env}"

[[ "$(id -u)" == 0 ]] || { echo "run as root" >&2; exit 1; }
[[ "$(realpath "$ROOT")" == /public/knowledgebase/observability ]] || {
  echo "unexpected observability root: $ROOT" >&2
  exit 1
}
[[ -f "$ROOT/compose.yml" && -f "$WEKNORA_ENV" ]] || {
  echo "missing observability bundle or WeKnora environment" >&2
  exit 1
}

for network in src_WeKnora-network kb-backplane; do
  docker network inspect "$network" >/dev/null
done
for container in WeKnora-postgres WeKnora-redis WeKnora-app kb-mineru-api; do
  docker inspect "$container" >/dev/null
done

read_env() {
  local key="$1"
  awk -F= -v key="$key" '$1 == key {sub(/^[^=]*=/, ""); print; exit}' "$WEKNORA_ENV"
}

db_name="$(read_env DB_NAME)"
redis_password="$(read_env REDIS_PASSWORD)"
[[ -n "$db_name" && -n "$redis_password" ]] || {
  echo "DB_NAME or REDIS_PASSWORD is missing from $WEKNORA_ENV" >&2
  exit 1
}
[[ "$db_name" =~ ^[A-Za-z0-9_]+$ ]] || { echo "unsafe DB_NAME" >&2; exit 1; }
[[ "$redis_password" =~ ^[A-Za-z0-9._~!@%+=:,-]+$ ]] || {
  echo "REDIS_PASSWORD contains characters that require manual env-file quoting" >&2
  exit 1
}

mkdir -p "$(dirname "$SECRETS_FILE")"
chmod 700 "$(dirname "$SECRETS_FILE")"
if [[ ! -f "$SECRETS_FILE" ]]; then
  umask 077
  observer_password="$(openssl rand -hex 24)"
  grafana_password="$(openssl rand -hex 24)"
  {
    printf 'MONITORED_HOST=marvel-kb\n'
    printf 'GRAFANA_BIND_ADDRESS=192.168.18.25\n'
    printf 'GRAFANA_ROOT_URL=http://192.168.18.25:13000\n'
    printf 'GRAFANA_ADMIN_USER=admin\n'
    printf 'GRAFANA_ADMIN_PASSWORD=%s\n' "$grafana_password"
    printf 'WEKNORA_DB_HOST=WeKnora-postgres\n'
    printf 'WEKNORA_DB_PORT=5432\n'
    printf 'WEKNORA_DB_NAME=%s\n' "$db_name"
    printf 'WEKNORA_DB_USER=weknora_observer\n'
    printf 'WEKNORA_DB_PASSWORD=%s\n' "$observer_password"
    printf 'WEKNORA_REDIS_HOST=WeKnora-redis\n'
    printf 'WEKNORA_REDIS_PORT=6379\n'
    printf 'WEKNORA_REDIS_PASSWORD=%s\n' "$redis_password"
    printf 'WEKNORA_NETWORK_NAME=src_WeKnora-network\n'
    printf 'KB_BACKPLANE_NETWORK_NAME=kb-backplane\n'
    printf 'GRAFANA_IMAGE=grafana/grafana:13.1.0\n'
    printf 'PROMETHEUS_IMAGE=prom/prometheus:v3.11.3\n'
    printf 'LOKI_IMAGE=grafana/loki:3.7.0\n'
    printf 'ALLOY_IMAGE=grafana/alloy:v1.18.0\n'
    printf 'PROMETHEUS_REMOTE_WRITE_URL=http://prometheus:9090/api/v1/write\n'
    printf 'LOKI_WRITE_URL=http://loki:3100/loki/api/v1/push\n'
    printf 'EXPECTED_CONTAINERS=WeKnora-postgres,WeKnora-redis,WeKnora-minio,WeKnora-docreader,WeKnora-app,WeKnora-frontend,kb-qdrant,kb-q4-gpu0,kb-q4-gpu1,kb-q4-router,kb-embed-text,kb-rerank-text,kb-mineru-api,WeKnora-mcp\n'
    printf 'MINERU_CONTAINER=kb-mineru-api\n'
    printf 'MINERU_PYTHON=python3\n'
    printf 'GPU_SOURCES=0:kb-q4-gpu0,1:kb-q4-gpu1\n'
    printf 'PROBE_INTERVAL_SECONDS=30\n'
    printf 'LOG_WINDOW_SECONDS=120\n'
  } >"$SECRETS_FILE"
  chmod 600 "$SECRETS_FILE"

  printf "%s\n" \
    "SELECT format('CREATE ROLE weknora_observer LOGIN PASSWORD %L', '$observer_password') WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname='weknora_observer') \\gexec" \
    "ALTER ROLE weknora_observer PASSWORD '$observer_password';" \
    "GRANT CONNECT ON DATABASE $db_name TO weknora_observer;" \
    "GRANT USAGE ON SCHEMA public TO weknora_observer;" \
    | docker exec -i WeKnora-postgres psql -U weknora -d "$db_name" -v ON_ERROR_STOP=1 >/dev/null
else
  chmod 600 "$SECRETS_FILE"
fi

docker exec -i WeKnora-postgres psql -U weknora -d "$db_name" -v ON_ERROR_STOP=1 \
  -v app_schema=public -v observer_schema=mwe_observer -v observer_role=weknora_observer \
  < "$ROOT/grafana/queries/observer-access.sql" >/dev/null

cd "$ROOT"
docker compose --env-file "$SECRETS_FILE" --profile backend --profile agent config --quiet
docker compose --env-file "$SECRETS_FILE" --profile backend --profile agent up -d --build

for _ in $(seq 1 60); do
  if curl -fsS http://127.0.0.1:13000/api/health >/dev/null \
    && curl -fsS http://127.0.0.1:19090/-/ready >/dev/null \
    && curl -fsS http://127.0.0.1:13100/ready >/dev/null \
    && curl -fsS http://127.0.0.1:12345/-/ready >/dev/null; then
    echo "OBSERVABILITY_DEPLOYMENT=ready"
    echo "GRAFANA_URL=http://127.0.0.1:13000"
    exit 0
  fi
  sleep 5
done

docker compose --env-file "$SECRETS_FILE" --profile backend --profile agent ps
echo "observability stack did not become ready" >&2
exit 1
