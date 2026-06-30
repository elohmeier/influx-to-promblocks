# influx-to-promblocks

[![CI](https://github.com/elohmeier/influx-to-promblocks/actions/workflows/ci.yml/badge.svg)](https://github.com/elohmeier/influx-to-promblocks/actions/workflows/ci.yml)
[![Release](https://github.com/elohmeier/influx-to-promblocks/actions/workflows/release.yml/badge.svg)](https://github.com/elohmeier/influx-to-promblocks/actions/workflows/release.yml)
[![GitHub release](https://img.shields.io/github/v/release/elohmeier/influx-to-promblocks?sort=semver)](https://github.com/elohmeier/influx-to-promblocks/releases)
[![Go Reference](https://pkg.go.dev/badge/github.com/elohmeier/influx-to-promblocks.svg)](https://pkg.go.dev/github.com/elohmeier/influx-to-promblocks)

`influx-to-promblocks` exports numeric data from an InfluxDB v1 HTTP API into local Prometheus TSDB blocks.

It is built for historical migration/backfill jobs:

1. Discover measurements and numeric fields with InfluxQL.
2. Stream data through chunked InfluxDB v1 queries, window by window.
3. Convert Influx fields into Prometheus series.
4. Write local Prometheus TSDB blocks.
5. Upload those blocks with the stock `thanos tools bucket upload-blocks` command.

## Build

```bash
go build ./cmd/influx-to-promblocks
```

## Version

```bash
influx-to-promblocks version
```

## Export

```bash
go run ./cmd/influx-to-promblocks export \
  --influx-url=http://localhost:8086 \
  --database=telegraf \
  --start=2024-01-01T00:00:00Z \
  --end=2024-01-02T00:00:00Z \
  --window=2h \
  --output-dir=./out/prom-blocks
```

The output directory is the handoff boundary. It contains Prometheus block directories and can be inspected, archived, checksummed, or uploaded independently.

## Upload

Use the Thanos CLI for object storage upload:

```bash
thanos tools bucket upload-blocks \
  --objstore.config-file=bucket.yml \
  --path=./out/prom-blocks \
  --label='cluster="legacy-influx"' \
  --label='replica="0"'
```

Useful flags:

- `--measurement=cpu`: restricts the copy to one measurement. Repeat for more.
- `--retention-policy=autogen`: selects an InfluxDB retention policy.
- `--parallelism=4`: copies multiple time windows concurrently. Start low and increase only while the source InfluxDB remains healthy.
- `--max-fields-per-query=50`: selects more fields in each Influx query, reducing HTTP round trips when measurements have many numeric fields.
- `--series-label=tenant=acme`: adds a static label to every generated Prometheus series.
- `--output-dir=./out/prom-blocks`: sets the Prometheus block output directory.
- `--metric-name-mode=field`: uses the Influx field name as the Prometheus metric name.
- `--include-booleans`: copies boolean fields as 0/1 gauges.
- `--duplicate-timestamp-policy=first`: drops later samples when multiple Influx nanosecond timestamps collapse into the same Prometheus millisecond.

## Performance tuning

Most export time is usually spent waiting for Influx `SELECT` queries. Block writes are local and normally much faster.

Useful levers:

- Use the largest `--window` that stays within `--block-duration` and does not make Influx responses too large. For example, `--window=48h --block-duration=48h` halves the number of windows compared with `--window=24h`.
- Use `--parallelism=N` to copy independent windows concurrently. Values like `2` or `4` are a reasonable starting point for backfills; higher values can overload the source InfluxDB.
- Increase `--max-fields-per-query` when many fields are exported from the same measurement. This reduces per-window query count, but very wide queries can increase Influx memory use.
- Tune `--chunk-size` upward only if the client is spending too much time decoding many small chunks and memory use remains acceptable.

## Mapping

By default, Influx measurement and field names become metric names:

- field `value`: `<measurement>`
- any other field: `<measurement>_<field>`

Use `--metric-name-mode=field` when the Influx field name already is the
Prometheus metric name. In that mode every field becomes `<field>`, including a
field named `value`.

For example, exporting a remote-write archive measurement such as
`remote_write_archive` can produce live-compatible metric names:

```bash
influx-to-promblocks export \
  --measurement=remote_write_archive \
  --metric-name-mode=field \
  --preserve-source-labels=false
```

This writes field `service_requests_total` as metric
`service_requests_total`, not `remote_write_archive_service_requests_total`.

Invalid Prometheus metric and label characters are replaced with `_`. Original source names are preserved as labels by default:

- `influxdb_measurement`
- `influxdb_field`

Influx tags become Prometheus labels. External labels are intentionally not written by this tool. Pass them to `thanos tools bucket upload-blocks` with repeated `--label` flags so Thanos owns the block-storage metadata step.

## Limitations

Prometheus stores sample timestamps at millisecond precision. InfluxDB v1 can store nanosecond timestamps. By default the copy fails if two different values in the same series collapse into the same Prometheus millisecond. Use `--duplicate-timestamp-policy=first` only if dropping later colliding samples is acceptable.

Only numeric Influx field types are copied by default: `float`, `integer`, and `unsigned`. String fields are skipped. Boolean fields require `--include-booleans`.

If copied blocks overlap existing blocks with the same Thanos external labels, Thanos compactor needs vertical compaction enabled for intentional backfills.

## Docker E2E

The repository includes a Docker setup with InfluxDB v1, MinIO, and Thanos tooling:

```bash
hack/e2e.sh
```

The script:

1. Starts InfluxDB v1 and MinIO.
2. Creates a MinIO bucket.
3. Seeds sample Influx line protocol.
4. Exports local Prometheus blocks.
5. Uploads those blocks with `thanos tools bucket upload-blocks`.
6. Uses `thanos tools bucket ls` in Docker to verify that a block exists.

Set `KEEP_E2E=1 ./hack/e2e.sh` to leave containers and volumes running after the check.

## Releases

Releases are automated on GitHub. Use Conventional Commit messages on `main`; Release Please opens and maintains a release PR with the next semantic version and `CHANGELOG.md` updates.

- `fix:` creates a patch release.
- `feat:` creates a minor release.
- `feat!:`, `fix!:`, or a `BREAKING CHANGE:` footer creates a major release.

When the release PR is merged, Release Please creates the tag and GitHub release. GoReleaser then builds Linux, macOS, and Windows artifacts and uploads them with `checksums.txt`.
