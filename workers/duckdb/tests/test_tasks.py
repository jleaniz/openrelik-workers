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
"""Tests for the jsonl_to_parquet task."""

import base64
import json

import duckdb
import pytest

from src import tasks


@pytest.fixture(autouse=True)
def _silence_celery_events(monkeypatch):
    """Stop ``self.send_event`` from trying to publish to a real broker."""
    for task in (tasks.jsonl_to_parquet, tasks.plaso_to_parquet):
        monkeypatch.setattr(task, "send_event", lambda *a, **k: None, raising=False)


def _input_file(tmp_path, name="abc.plaso.json_line", lines=3):
    path = tmp_path / f"in-{name}"
    with open(path, "w") as fh:
        for i in range(lines):
            fh.write(json.dumps({"timestamp": i + 1, "message": f"m{i}", "data_type": "x"}) + "\n")
    return {
        "id": 1,
        "uuid": "u",
        "display_name": name,
        "data_type": "plaso:psort:json_line",
        "path": str(path),
    }


def _run(tmp_path, input_files, task_config=None):
    out = tmp_path / "out"
    out.mkdir()
    encoded = tasks.jsonl_to_parquet.run(
        input_files=input_files, output_path=str(out), workflow_id=1, task_config=task_config
    )
    return json.loads(base64.b64decode(encoded))


def test_converts_json_lines_and_names_output(tmp_path):
    result = _run(tmp_path, [_input_file(tmp_path)])

    [output] = result["output_files"]
    assert output["display_name"] == "abc.plaso.parquet"
    assert output["data_type"] == "openrelik:duckdb:parquet"
    assert output["path"].endswith(".parquet")
    assert output["register_in_db"] is True
    assert result["meta"]["rows"] == {"abc.plaso.json_line": 3}
    assert duckdb.connect().execute(f"SELECT count(*) FROM '{output['path']}'").fetchone() == (3,)


def test_output_is_always_registered(tmp_path):
    """The Parquet file must be registered for the UI's SQL viewer, whatever the config says."""
    result = _run(tmp_path, [_input_file(tmp_path)], {"register_in_db": False})

    assert result["output_files"][0]["register_in_db"] is True


def test_register_in_db_is_not_a_task_option():
    assert "register_in_db" not in {o["name"] for o in tasks.TASK_METADATA["task_config"]}


def test_non_json_lines_inputs_are_ignored(tmp_path):
    other = _input_file(tmp_path, name="disk.plaso")
    other["data_type"] = "plaso:log2timeline:plaso_storage"

    result = _run(tmp_path, [other])

    assert result["output_files"] == []


def test_plaso_to_parquet_task(tmp_path):
    from tests.test_plaso_conversion import FILE_DATA, _make_plaso

    src = tmp_path / "abc.plaso"
    _make_plaso(str(src), [FILE_DATA], [(1, 10, "Creation Time"), (1, 20, "Last Access Time")])
    out = tmp_path / "out"
    out.mkdir()
    input_file = {
        "id": 1,
        "uuid": "u",
        "display_name": "abc.plaso",
        "data_type": "plaso:log2timeline:plaso_storage",
        "path": str(src),
    }

    result = json.loads(
        base64.b64decode(
            tasks.plaso_to_parquet.run(
                input_files=[input_file], output_path=str(out), workflow_id=1, task_config={}
            )
        )
    )

    [output] = result["output_files"]
    assert output["display_name"] == "abc.parquet"
    assert output["data_type"] == "openrelik:duckdb:plaso_parquet"
    assert output["register_in_db"] is True
    assert result["meta"]["rows"] == {"abc.plaso": 2}


def test_plaso_to_parquet_ignores_other_inputs(tmp_path):
    other = _input_file(tmp_path)  # a JSON lines file

    out = tmp_path / "out"
    out.mkdir()
    result = json.loads(
        base64.b64decode(
            tasks.plaso_to_parquet.run(
                input_files=[other], output_path=str(out), workflow_id=1, task_config={}
            )
        )
    )

    assert result["output_files"] == []
