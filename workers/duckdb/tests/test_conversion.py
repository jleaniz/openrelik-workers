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
"""Tests for the JSON lines to Parquet conversion."""

import json
import os

import duckdb
import pytest

from src.conversion import CORE_FIELDS, convert_jsonl_to_parquet


def _event(**fields):
    event = {
        "__container_type__": "event",
        "__type__": "AttributeContainer",
        "data_type": "fs:stat:ntfs",
        "display_name": "NTFS:\\Windows\\a.txt",
        "filename": "\\Windows\\a.txt",
        "inode": "42",
        "message": "a message",
        "parser": "mft",
        "sha256_hash": "abc",
        "timestamp": 1712345678000000,
        "timestamp_desc": "Creation Time",
        "date_time": {"__type__": "DateTimeValues", "timestamp": 1},
    }
    event.update(fields)
    return event


def _write_jsonl(path, events):
    with open(path, "w") as fh:
        for event in events:
            fh.write(json.dumps(event) + "\n")


def _read(path, sql):
    con = duckdb.connect()
    try:
        return con.execute(sql.replace("{f}", f"'{path}'")).fetchall()
    finally:
        con.close()


@pytest.fixture
def paths(tmp_path):
    return str(tmp_path / "in.json_line"), str(tmp_path / "out.parquet")


def test_schema_is_core_columns_plus_attributes(paths):
    src, dst = paths
    _write_jsonl(src, [_event()])

    rows = convert_jsonl_to_parquet(src, dst)

    assert rows == 1
    columns = [c[0] for c in _read(dst, "DESCRIBE SELECT * FROM {f}")]
    assert columns == ["datetime", "timestamp", *CORE_FIELDS, "attributes"]
    assert not os.path.exists(f"{dst}.partial")


def test_values_and_attributes(paths):
    src, dst = paths
    _write_jsonl(src, [_event(key_path="HKLM\\Software")])

    convert_jsonl_to_parquet(src, dst)

    [(dt, ts, data_type, message, key_path, has_message, has_type)] = _read(
        dst,
        "SELECT datetime, timestamp, data_type, message, attributes->>'key_path', "
        "json_exists(attributes, '$.message'), json_exists(attributes, '$.__type__') FROM {f}",
    )
    assert str(dt) == "2024-04-05 19:34:38"
    assert ts == 1712345678000000
    assert (data_type, message, key_path) == ("fs:stat:ntfs", "a message", "HKLM\\Software")
    # Core fields and plaso's type markers aren't repeated in attributes.
    assert (has_message, has_type) == (False, False)


@pytest.mark.parametrize("timestamp", [0, -5, "not a number", None])
def test_unset_or_invalid_timestamp_gives_null_datetime(paths, timestamp):
    src, dst = paths
    _write_jsonl(src, [_event(timestamp=timestamp)])

    convert_jsonl_to_parquet(src, dst)

    [(dt,)] = _read(dst, "SELECT datetime FROM {f}")
    assert dt is None


def test_fields_first_seen_late_are_kept(paths):
    """Unlike read_json_auto's sampled schema, a field first seen late isn't lost."""
    src, dst = paths
    events = [_event() for _ in range(30000)] + [_event(control_set=7)]
    _write_jsonl(src, events)

    rows = convert_jsonl_to_parquet(src, dst, row_group_size=4096)

    assert rows == 30001
    assert _read(dst, "SELECT count(*) FROM {f} WHERE attributes->>'control_set' = '7'") == [(1,)]


def test_order_is_preserved(paths):
    src, dst = paths
    _write_jsonl(src, [_event(timestamp=t, message=str(t)) for t in range(1, 2001)])

    convert_jsonl_to_parquet(src, dst, threads=4, row_group_size=100)

    messages = [m for (m,) in _read(dst, "SELECT message FROM {f}")]
    assert messages == [str(t) for t in range(1, 2001)]


def test_failed_conversion_leaves_no_output(paths):
    src, dst = paths
    with open(src, "w") as fh:
        fh.write("{not json\n")

    with pytest.raises(duckdb.Error):
        convert_jsonl_to_parquet(src, dst)

    assert not os.path.exists(dst)
    assert not os.path.exists(f"{dst}.partial")


@pytest.mark.parametrize(
    "kwargs", [{"compression": "lz4-bogus"}, {"row_group_size": 0}]
)
def test_invalid_options_are_rejected(paths, kwargs):
    src, dst = paths
    _write_jsonl(src, [_event()])

    with pytest.raises(ValueError):
        convert_jsonl_to_parquet(src, dst, **kwargs)


def test_quotes_in_paths(tmp_path):
    src = str(tmp_path / "it's.json_line")
    dst = str(tmp_path / "it's.parquet")
    _write_jsonl(src, [_event()])

    assert convert_jsonl_to_parquet(src, dst) == 1
