#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

cleanup() {
  if [[ "${KEEP_E2E:-}" != "1" ]]; then
    docker compose down -v --remove-orphans >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

docker compose up -d influxdb minio
docker compose run --rm create-bucket

deadline=$((SECONDS + ${E2E_STARTUP_TIMEOUT_SECONDS:-90}))
until curl -fsS http://localhost:8086/ping >/dev/null; do
  if (( SECONDS >= deadline )); then
    echo "timed out waiting for InfluxDB" >&2
    docker compose logs influxdb minio >&2 || true
    exit 1
  fi
  sleep 1
done

curl -fsS -G http://localhost:8086/query --data-urlencode "q=CREATE DATABASE telegraf" >/dev/null
curl -fsS -XPOST "http://localhost:8086/write?db=telegraf&precision=ns" --data-binary "@hack/seed.lp"

rm -rf .tmp/e2e-blocks
mkdir -p .tmp/e2e-blocks

go run ./cmd/influx-to-promblocks export \
  --influx-url=http://localhost:8086 \
  --database=telegraf \
  --start=2024-01-01T00:00:00Z \
  --end=2024-01-01T02:00:00Z \
  --window=2h \
  --output-dir=.tmp/e2e-blocks

docker compose --profile tools run --rm thanos tools bucket upload-blocks \
  --objstore.config-file=/etc/thanos/bucket.yml \
  --path=/blocks \
  --label='cluster="docker-e2e"' \
  --label='replica="0"'

docker compose --profile tools run --rm thanos tools bucket ls --objstore.config-file=/etc/thanos/bucket.yml | tee .tmp/e2e-bucket-ls.txt

if ! grep -Eq '[0-9A-Z]{26}' .tmp/e2e-bucket-ls.txt; then
  echo "expected at least one Thanos block in bucket" >&2
  exit 1
fi

echo "e2e ok"
