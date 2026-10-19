# Openrelik worker: Plaso JSON lines to Parquet

Converts plaso timelines in JSON lines format (`psort -o json_line`) to Parquet
using DuckDB. The task is `openrelik-worker-duckdb.tasks.jsonl_to_parquet`.

## Output schema

| Column | Type | Source |
|---|---|---|
| `datetime` | TIMESTAMP | `timestamp` (microseconds); NULL when not set |
| `timestamp` | BIGINT | `timestamp` |
| `timestamp_desc`, `data_type`, `parser`, `display_name`, `filename`, `inode`, `message`, `sha256_hash` | VARCHAR | same-named fields |
| `attributes` | JSON | every other field of the event |

Plaso events have different fields per `data_type`, often more than 100 in one
timeline, so everything outside the common fields goes into `attributes`. Query
them with DuckDB's JSON operators, e.g. `attributes->>'key_path'`.

The output is named after the input with a `.parquet` extension and has data
type `openrelik:duckdb:parquet`. It is always registered in OpenRelik, so it can
be queried in the UI's SQL viewer.

## Plaso storage to Parquet

`openrelik-worker-duckdb.tasks.plaso_to_parquet` converts a `.plaso` storage
file directly to Parquet with the same columns, without running psort. A
`.plaso` file is SQLite; its event data is zlib-compressed JSON that SQL can't
decode, so the task reads it with Python and writes Parquet with pyarrow.
plaso writes event data before the events that use it, so both tables are read
in order and merged as they stream, in one process: memory stays flat whatever
the file size.

Compared with psort's output:

- `message` is always empty (psort builds it from plaso's formatters).
- `display_name` and `filename` fall back to the source file's path spec where
  plaso didn't store them.
- duplicate events are kept (psort drops them).
- rows are in storage order, not time order: add `ORDER BY timestamp` when
  querying.

On a 1.5 GB `.plaso` file with 3.7M events (psort took 747 s on it) the
conversion took 45 s with a peak of 0.7 GB.

Task option: `compression`. The output is always registered in OpenRelik.

## Resources

For the JSON lines task, DuckDB streams the input: memory depends on threads,
row group size and event size, not on the file size. Measured on a 7.6 GB, 3.7M-event timeline:

| Threads | Memory limit | Row group | Time | Peak memory |
|---|---|---|---|---|
| 1 | 1 GB | 16,384 | 67 s | 0.7 GB |
| 2 | 1 GB | 16,384 | 35 s | 1.2 GB |
| 4 | 2 GB | 16,384 | 18 s | 2.2 GB |

The Parquet file was 109 MB. Actual memory runs about 150-200 MB above the
DuckDB limit, so size the container limit with headroom.

## Configuration

Environment variables:

- `DUCKDB_THREADS` (default `2`)
- `DUCKDB_MEMORY_LIMIT` (default `1GB`)

Both apply to the JSON lines task only.

Task options: `compression` (`zstd` default, `snappy`, `gzip`, `uncompressed`) and
`row_group_size` (default `16384`).

## Deploy

```
openrelik-worker-duckdb:
    container_name: openrelik-worker-duckdb
    image: ghcr.io/openrelik/openrelik-worker-duckdb:latest
    restart: always
    environment:
      - REDIS_URL=redis://openrelik-redis:6379
      - DUCKDB_THREADS=2
      - DUCKDB_MEMORY_LIMIT=1GB
    volumes:
      - ./data:/usr/share/openrelik/data
    command: "celery --app=src.app worker --task-events --concurrency=1 --loglevel=INFO -Q openrelik-worker-duckdb"
```

Keep `--concurrency=1`, or multiply the memory limit by the concurrency.

## Test
```
uv sync --group test
uv run pytest -s --cov=.
```
