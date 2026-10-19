# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Convert plaso JSON lines (psort -o json_line) to Parquet with DuckDB.

The output has typed columns for the fields every plaso event has, plus an
``attributes`` JSON column with all other fields. Plaso events have different
fields per data_type (100+ in a typical timeline), so a fixed set of columns
keeps the schema the same across files.

DuckDB streams the input, so memory use depends on threads, row group size and
event size, not on the input size.
"""

import os

import duckdb

# Fields present on every plaso event, stored as their own columns.
CORE_FIELDS = [
    "timestamp_desc",
    "data_type",
    "parser",
    "display_name",
    "filename",
    "inode",
    "message",
    "sha256_hash",
]
# Fields dropped from ``attributes``: the core fields, and plaso's constant type markers.
_DROPPED_FIELDS = ["timestamp", *CORE_FIELDS, "__container_type__", "__type__"]

# Largest microsecond value make_timestamp accepts (9999-12-31).
_MAX_TIMESTAMP_US = 253402300799999999

VALID_COMPRESSION = ("zstd", "snappy", "gzip", "uncompressed")


def _quote(path: str) -> str:
    """Return ``path`` as a SQL string literal."""
    return "'" + path.replace("'", "''") + "'"


def conversion_sql(src: str, dst: str, compression: str, row_group_size: int) -> str:
    """Return the DuckDB COPY statement that converts ``src`` to Parquet at ``dst``."""
    drop_patch = "{" + ",".join(f'"{name}":null' for name in _DROPPED_FIELDS) + "}"
    core_columns = ",\n    ".join(f"json->>'{name}' AS {name}" for name in CORE_FIELDS)
    return f"""
COPY (
  SELECT
    CASE WHEN TRY_CAST(json->>'timestamp' AS BIGINT) BETWEEN 1 AND {_MAX_TIMESTAMP_US}
         THEN make_timestamp(TRY_CAST(json->>'timestamp' AS BIGINT)) END AS datetime,
    TRY_CAST(json->>'timestamp' AS BIGINT) AS timestamp,
    {core_columns},
    json_merge_patch(json, '{drop_patch}') AS attributes
  FROM read_ndjson_objects({_quote(src)})
) TO {_quote(dst)} (FORMAT parquet, COMPRESSION {compression}, ROW_GROUP_SIZE {row_group_size})
"""


def convert_jsonl_to_parquet(
    src: str,
    dst: str,
    *,
    compression: str = "zstd",
    row_group_size: int = 16384,
    threads: int = 2,
    memory_limit: str = "1GB",
    temp_directory: str | None = None,
) -> int:
    """Convert a JSON lines file to Parquet and return the number of rows written.

    The file is written to ``dst + ".partial"`` and renamed into place, so a
    failed conversion never leaves a truncated Parquet file at ``dst``.

    Args:
        src: Path to the JSON lines input.
        dst: Path of the Parquet file to write.
        compression: One of VALID_COMPRESSION.
        row_group_size: Rows per Parquet row group. Each thread buffers about one
            row group, so smaller values use less memory.
        threads: DuckDB worker threads.
        memory_limit: DuckDB memory limit, e.g. "1GB". Actual use runs about
            150-200 MB above it.
        temp_directory: Where DuckDB may spill to disk, if it needs to.

    Raises:
        ValueError: On an invalid compression or row group size.
    """
    if compression not in VALID_COMPRESSION:
        raise ValueError(f"compression must be one of {VALID_COMPRESSION}, got {compression!r}")
    if row_group_size <= 0:
        raise ValueError(f"row_group_size must be positive, got {row_group_size}")

    partial = f"{dst}.partial"
    con = duckdb.connect()
    try:
        con.execute(f"SET threads = {int(threads)}")
        con.execute(f"SET memory_limit = {_quote(memory_limit)}")
        # psort output is already in time order; keep it.
        con.execute("SET preserve_insertion_order = true")
        if temp_directory:
            os.makedirs(temp_directory, exist_ok=True)
            con.execute(f"SET temp_directory = {_quote(temp_directory)}")
        rows = con.execute(conversion_sql(src, partial, compression, row_group_size)).fetchone()[0]
        os.replace(partial, dst)
        return rows
    except Exception:
        if os.path.exists(partial):
            os.unlink(partial)
        raise
    finally:
        con.close()
