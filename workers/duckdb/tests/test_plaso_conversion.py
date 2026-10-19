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
"""Tests for the .plaso to Parquet conversion, on synthetic plaso storage files."""

import json
import os
import sqlite3
import zlib

import duckdb
import pytest

from src.conversion import CORE_FIELDS
from src.plaso_conversion import UnsupportedPlasoStorage, convert_plaso_to_parquet

PATH_SPEC = {"__type__": "PathSpec", "type_indicator": "NTFS", "location": "\\Windows\\a.evtx"}


def _make_plaso(
    path,
    event_data,
    events,
    *,
    compression="zlib",
    serialization="json",
    streams=((1, "abc123", PATH_SPEC),),
):
    """Write a minimal plaso SQLite storage file.

    Args:
        event_data: list of dicts, stored as event_data rows 1..N.
        events: list of (event_data_id, timestamp, timestamp_desc).
    """
    con = sqlite3.connect(path)
    con.executescript(
        """
        CREATE TABLE metadata (key TEXT, value TEXT);
        CREATE TABLE event (_identifier INTEGER PRIMARY KEY AUTOINCREMENT,
            _event_data_identifier TEXT, date_time TEXT, timestamp BIGINT, timestamp_desc TEXT);
        CREATE TABLE event_data (_identifier INTEGER PRIMARY KEY AUTOINCREMENT, _data BLOB);
        CREATE TABLE event_data_stream (_identifier INTEGER PRIMARY KEY AUTOINCREMENT,
            file_entropy TEXT, md5_hash TEXT, path_spec TEXT, sha1_hash TEXT,
            sha256_hash TEXT, yara_match TEXT);
        """
    )
    con.executemany(
        "INSERT INTO metadata VALUES (?, ?)",
        [
            ("format_version", "20230327"),
            ("compression_format", compression),
            ("serialization_format", serialization),
        ],
    )
    for identifier, sha256_hash, spec in streams:
        con.execute(
            "INSERT INTO event_data_stream (_identifier, path_spec, sha256_hash) VALUES (?, ?, ?)",
            (identifier, json.dumps(spec), sha256_hash),
        )
    for data in event_data:
        raw = json.dumps({"__container_type__": "event_data", "__type__": "AttributeContainer", **data})
        con.execute(
            "INSERT INTO event_data (_data) VALUES (?)",
            (zlib.compress(raw.encode()) if compression == "zlib" else raw.encode(),),
        )
    con.executemany(
        "INSERT INTO event (_event_data_identifier, date_time, timestamp, timestamp_desc)"
        " VALUES (?, '{}', ?, ?)",
        [(f"event_data.{data_id}", ts, desc) for data_id, ts, desc in events],
    )
    con.commit()
    con.close()


def _query(path, sql):
    con = duckdb.connect()
    try:
        return con.execute(sql.replace("{f}", f"'{path}'")).fetchall()
    finally:
        con.close()


FILE_DATA = {
    "data_type": "fs:stat",
    "_parser_chain": "filestat",
    "display_name": "NTFS:\\Windows\\a.txt",
    "filename": "\\Windows\\a.txt",
    "inode": 42,
    "_event_values_hash": "h1",
    "_event_data_stream_identifier": "event_data_stream.1",
    "file_size": 10,
}
REGISTRY_DATA = {
    "data_type": "windows:registry:key_value",
    "_parser_chain": "winreg/winreg_default",
    "key_path": "HKLM\\Software",
    "_event_data_stream_identifier": "event_data_stream.1",
}


@pytest.fixture
def paths(tmp_path):
    return str(tmp_path / "in.plaso"), str(tmp_path / "out.parquet")


def test_schema_matches_json_lines_conversion(paths):
    src, dst = paths
    _make_plaso(src, [FILE_DATA], [(1, 1712345678000000, "Creation Time")])

    assert convert_plaso_to_parquet(src, dst) == 1

    columns = [c[0] for c in _query(dst, "DESCRIBE SELECT * FROM {f}")]
    assert columns == ["datetime", "timestamp", *CORE_FIELDS, "attributes"]
    assert not any(name.endswith(".partial") for name in os.listdir(os.path.dirname(dst)))


def test_values(paths):
    src, dst = paths
    _make_plaso(src, [FILE_DATA], [(1, 1712345678000000, "Creation Time")])

    convert_plaso_to_parquet(src, dst)

    [row] = _query(
        dst,
        "SELECT datetime::VARCHAR, timestamp, timestamp_desc, data_type, parser, display_name,"
        " filename, inode, message, sha256_hash, attributes FROM {f}",
    )
    attributes = json.loads(row[-1])
    assert row[:-1] == (
        "2024-04-05 19:34:38",
        1712345678000000,
        "Creation Time",
        "fs:stat",
        "filestat",
        "NTFS:\\Windows\\a.txt",
        "\\Windows\\a.txt",
        "42",
        None,
        "abc123",
    )
    assert attributes["file_size"] == 10
    assert attributes["pathspec"] == PATH_SPEC
    # Private and column fields aren't repeated in attributes.
    assert not {"_event_values_hash", "data_type", "display_name", "__type__"} & set(attributes)


def test_display_name_and_filename_fall_back_to_stream(paths):
    src, dst = paths
    _make_plaso(src, [REGISTRY_DATA], [(1, 1, "Last Written Time")])

    convert_plaso_to_parquet(src, dst)

    assert _query(dst, "SELECT display_name, filename, attributes->>'key_path' FROM {f}") == [
        ("NTFS:\\Windows\\a.evtx", "\\Windows\\a.evtx", "HKLM\\Software")
    ]


def test_several_events_share_one_event_data(paths):
    src, dst = paths
    _make_plaso(
        src,
        [FILE_DATA, REGISTRY_DATA],
        [(1, 10, "Creation Time"), (1, 20, "Last Access Time"), (2, 30, "Last Written Time")],
    )

    convert_plaso_to_parquet(src, dst)

    assert _query(dst, "SELECT timestamp, data_type FROM {f}") == [
        (10, "fs:stat"),
        (20, "fs:stat"),
        (30, "windows:registry:key_value"),
    ]


def test_out_of_order_reference_is_looked_up(paths):
    """An event pointing back to an earlier event_data row is still converted."""
    src, dst = paths
    _make_plaso(
        src,
        [FILE_DATA, REGISTRY_DATA],
        [(2, 30, "Last Written Time"), (1, 10, "Creation Time")],
    )

    convert_plaso_to_parquet(src, dst)

    assert _query(dst, "SELECT timestamp, data_type FROM {f}") == [
        (30, "windows:registry:key_value"),
        (10, "fs:stat"),
    ]


def test_missing_event_data_fails_without_output(paths):
    src, dst = paths
    _make_plaso(src, [FILE_DATA], [(1, 10, "Creation Time"), (5, 20, "Creation Time")])

    with pytest.raises(ValueError, match="missing event_data.5"):
        convert_plaso_to_parquet(src, dst)

    assert os.listdir(os.path.dirname(dst)) == ["in.plaso"]


@pytest.mark.parametrize("timestamp", [0, -1, None])
def test_unset_timestamp_gives_null_datetime(paths, timestamp):
    src, dst = paths
    _make_plaso(src, [FILE_DATA], [(1, timestamp, "Not a time")])

    convert_plaso_to_parquet(src, dst)

    assert _query(dst, "SELECT datetime FROM {f}") == [(None,)]


def test_uncompressed_storage(paths):
    src, dst = paths
    _make_plaso(src, [FILE_DATA], [(1, 10, "Creation Time")], compression="none")

    assert convert_plaso_to_parquet(src, dst) == 1


@pytest.mark.parametrize(
    "kwargs, match",
    [
        ({"compression": "bz2"}, "compression format"),
        ({"serialization": "protobuf"}, "serialization format"),
    ],
)
def test_unsupported_storage_is_rejected(paths, kwargs, match):
    src, dst = paths
    _make_plaso(src, [FILE_DATA], [(1, 10, "Creation Time")], **kwargs)

    with pytest.raises(UnsupportedPlasoStorage, match=match):
        convert_plaso_to_parquet(src, dst)


def test_not_a_plaso_file(paths):
    src, dst = paths
    with open(src, "w") as fh:
        fh.write("not sqlite")

    with pytest.raises(UnsupportedPlasoStorage):
        convert_plaso_to_parquet(src, dst)


def test_output_spans_several_batches(paths):
    src, dst = paths
    n_data = 300
    _make_plaso(
        src,
        [{**FILE_DATA, "inode": i} for i in range(1, n_data + 1)],
        [(i, (n_data - i) * 10 + j, f"desc{j}") for i in range(1, n_data + 1) for j in range(3)],
    )

    assert convert_plaso_to_parquet(src, dst, batch_rows=50) == 900

    # Rows stay in storage order.
    rows = _query(dst, "SELECT inode, timestamp_desc FROM {f}")
    assert rows == [(str(i), f"desc{j}") for i in range(1, n_data + 1) for j in range(3)]


@pytest.mark.parametrize("compression", ["zstd", "snappy", "gzip", "uncompressed"])
def test_output_compression(paths, compression):
    src, dst = paths
    _make_plaso(src, [FILE_DATA], [(1, 10, "Creation Time")])

    assert convert_plaso_to_parquet(src, dst, compression=compression) == 1

    [(codec,)] = _query(dst, "SELECT DISTINCT compression FROM parquet_metadata({f})")
    assert codec.lower() == compression


@pytest.mark.parametrize("kwargs", [{"compression": "lz4-bogus"}, {"batch_rows": 0}])
def test_invalid_options(paths, kwargs):
    src, dst = paths
    _make_plaso(src, [FILE_DATA], [(1, 10, "Creation Time")])

    with pytest.raises(ValueError):
        convert_plaso_to_parquet(src, dst, **kwargs)


def test_malformed_reference_fails_without_output(paths):
    src, dst = paths
    _make_plaso(src, [FILE_DATA, FILE_DATA], [(1, 1, "a"), (2, 2, "b")])
    con = sqlite3.connect(src)
    con.execute("UPDATE event SET _event_data_identifier = 'garbage' WHERE _identifier = 2")
    con.commit()
    con.close()

    with pytest.raises(ValueError, match="Unexpected event data reference"):
        convert_plaso_to_parquet(src, dst)
    assert os.listdir(os.path.dirname(dst)) == ["in.plaso"]
