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
"""Convert a plaso storage file (.plaso) directly to Parquet, without psort.

A .plaso file is SQLite. Each row of ``event`` points to a row of ``event_data``
("event_data.<N>"), whose ``_data`` column holds the event's attributes as
(usually zlib-compressed) JSON. plaso writes event data before the events that
use it, so both tables are read in id order and merged as they stream: each
event_data row is decoded once, and memory stays flat regardless of file size.
An event that points back to an earlier event_data row is still handled, by
looking that row up by id.

The output has the same columns as the JSON lines conversion (see
conversion.py). Fields that psort computes at output time differ:
  - ``message`` is always NULL (psort builds it from plaso's formatters).
  - ``display_name`` and ``filename`` fall back to the event data stream's path
    spec where the event data has none.
  - duplicate events are kept (psort drops them).
  - rows are in storage order, not time order; sort at query time.
"""

import json
import os
import sqlite3
import zlib
from functools import lru_cache

import pyarrow as pa
import pyarrow.parquet as pq

from .conversion import CORE_FIELDS, VALID_COMPRESSION

SCHEMA = pa.schema(
    [("datetime", pa.timestamp("us")), ("timestamp", pa.int64())]
    + [(name, pa.string()) for name in CORE_FIELDS]
    + [("attributes", pa.string())]
)

# Event data fields stored as columns rather than in ``attributes``.
_COLUMN_FIELDS = {"data_type", "display_name", "filename", "inode"}

# Largest microsecond value a Parquet timestamp column accepts (9999-12-31).
_MAX_TIMESTAMP_US = 253402300799999999

_EVENT_DATA_PREFIX = "event_data."
_STREAM_PREFIX = "event_data_stream."

SUPPORTED_COMPRESSION_FORMATS = ("zlib", "none")

# pyarrow's names for VALID_COMPRESSION, where they differ.
_PYARROW_COMPRESSION = {"uncompressed": "none"}


class UnsupportedPlasoStorage(ValueError):
    """The .plaso file uses a storage format this converter can't read."""


def _check_storage(con: sqlite3.Connection) -> bool:
    """Validate the storage metadata and return True if event data is zlib-compressed."""
    try:
        metadata = dict(con.execute("SELECT key, value FROM metadata").fetchall())
    except sqlite3.DatabaseError as e:
        raise UnsupportedPlasoStorage(f"Not a plaso SQLite storage file: {e}") from e
    if metadata.get("serialization_format") != "json":
        raise UnsupportedPlasoStorage(
            f"Unsupported serialization format {metadata.get('serialization_format')!r}"
        )
    compression = metadata.get("compression_format") or "none"
    if compression not in SUPPORTED_COMPRESSION_FORMATS:
        raise UnsupportedPlasoStorage(f"Unsupported compression format {compression!r}")
    return compression == "zlib"


def _read_streams(con: sqlite3.Connection) -> dict:
    """Return event data stream fields by identifier ("event_data_stream.<N>")."""
    streams = {}
    for identifier, sha256_hash, path_spec in con.execute(
        "SELECT _identifier, sha256_hash, path_spec FROM event_data_stream"
    ):
        try:
            spec = json.loads(path_spec) if path_spec else {}
        except ValueError:
            spec = {}
        location = spec.get("location")
        display_name = f"{spec.get('type_indicator', 'OS')}:{location}" if location else None
        streams[f"{_STREAM_PREFIX}{identifier}"] = (sha256_hash, display_name, location, spec)
    return streams


class _EventDataReader:
    """Decodes event_data rows, sequentially where possible and by id otherwise."""

    def __init__(self, con: sqlite3.Connection, compressed: bool, streams: dict):
        self._compressed = compressed
        self._streams = streams
        self._rows = con.cursor().execute(
            "SELECT _identifier, _data FROM event_data ORDER BY _identifier"
        )
        self._lookup_cursor = con.cursor()
        self._current_id = 0
        self._current = None
        self.lookups = 0
        self._lookup = lru_cache(maxsize=4096)(self._lookup_uncached)

    def _decode(self, blob) -> tuple:
        data = json.loads(zlib.decompress(blob) if self._compressed else blob)
        sha256_hash, stream_display_name, stream_location, path_spec = self._streams.get(
            data.get("_event_data_stream_identifier"), (None, None, None, None)
        )
        inode = data.get("inode")
        attributes = {
            key: value
            for key, value in data.items()
            if not key.startswith("_") and key not in _COLUMN_FIELDS
        }
        if path_spec:
            attributes["pathspec"] = path_spec
        return (
            data.get("data_type"),
            data.get("_parser_chain"),
            data.get("display_name") or stream_display_name,
            data.get("filename") or stream_location,
            None if inode is None else str(inode),
            sha256_hash,
            json.dumps(attributes),
        )

    def _lookup_uncached(self, data_id: int) -> tuple:
        row = self._lookup_cursor.execute(
            "SELECT _data FROM event_data WHERE _identifier = ?", (data_id,)
        ).fetchone()
        if row is None:
            raise ValueError(f"Event references missing {_EVENT_DATA_PREFIX}{data_id}")
        return self._decode(row[0])

    def get(self, data_id: int) -> tuple:
        if data_id < self._current_id:
            # Out of order: plaso doesn't normally write this, but handle it anyway.
            self.lookups += 1
            return self._lookup(data_id)
        while self._current_id < data_id:
            row = next(self._rows, None)
            if row is None:
                break
            self._current_id, blob = row
            if self._current_id == data_id:
                self._current = self._decode(blob)
        if self._current_id != data_id:
            raise ValueError(f"Event references missing {_EVENT_DATA_PREFIX}{data_id}")
        return self._current


def convert_plaso_to_parquet(
    src: str, dst: str, *, compression: str = "zstd", batch_rows: int = 65536
) -> int:
    """Convert a .plaso storage file to Parquet and return the number of rows.

    The file is written to ``dst + ".partial"`` and renamed into place, so a
    failure never leaves a partial ``dst``.

    Args:
        src: Path to the .plaso file.
        dst: Path of the Parquet file to write.
        compression: One of VALID_COMPRESSION.
        batch_rows: Rows per Parquet row group.

    Raises:
        UnsupportedPlasoStorage: If the file isn't a supported plaso storage file.
        ValueError: On invalid options or a broken event reference.
    """
    if compression not in VALID_COMPRESSION:
        raise ValueError(f"compression must be one of {VALID_COMPRESSION}, got {compression!r}")
    if batch_rows < 1:
        raise ValueError(f"batch_rows must be positive, got {batch_rows}")

    partial = f"{dst}.partial"
    con = sqlite3.connect(f"file:{src}?mode=ro", uri=True)
    try:
        compressed = _check_storage(con)
        reader = _EventDataReader(con, compressed, _read_streams(con))
        events = con.execute(
            "SELECT _event_data_identifier, timestamp, timestamp_desc FROM event"
            " ORDER BY _identifier"
        )
        columns = {name: [] for name in SCHEMA.names}
        names = ["data_type", "parser", "display_name", "filename", "inode", "sha256_hash"]
        rows = 0
        writer_compression = _PYARROW_COMPRESSION.get(compression, compression)
        with pq.ParquetWriter(partial, SCHEMA, compression=writer_compression) as writer:
            for reference, timestamp, timestamp_desc in events:
                if not reference or not reference.startswith(_EVENT_DATA_PREFIX):
                    raise ValueError(f"Unexpected event data reference {reference!r}")
                values = reader.get(int(reference[len(_EVENT_DATA_PREFIX) :]))
                valid = isinstance(timestamp, int) and 0 < timestamp <= _MAX_TIMESTAMP_US
                columns["datetime"].append(timestamp if valid else None)
                columns["timestamp"].append(timestamp if isinstance(timestamp, int) else None)
                columns["timestamp_desc"].append(timestamp_desc)
                columns["message"].append(None)
                for name, value in zip(names, values):
                    columns[name].append(value)
                columns["attributes"].append(values[-1])
                rows += 1
                if rows % batch_rows == 0:
                    writer.write_batch(pa.RecordBatch.from_pydict(columns, schema=SCHEMA))
                    for values_list in columns.values():
                        values_list.clear()
            if columns["timestamp"]:
                writer.write_batch(pa.RecordBatch.from_pydict(columns, schema=SCHEMA))
        os.replace(partial, dst)
        return rows
    finally:
        con.close()
        if os.path.exists(partial):
            os.unlink(partial)
