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

import os

from celery import signals
from celery.utils.log import get_task_logger

from openrelik_common import telemetry
from openrelik_common.logging import Logger
from openrelik_worker_common.file_utils import create_output_file
from openrelik_worker_common.task_utils import create_task_result, get_input_files

from .app import celery
from .conversion import VALID_COMPRESSION, convert_jsonl_to_parquet
from .plaso_conversion import convert_plaso_to_parquet

# Task name used to register and route the task to the correct queue.
TASK_NAME = "openrelik-worker-duckdb.tasks.jsonl_to_parquet"

# Resource settings come from the deployment, so they can match the pod's limits.
DUCKDB_THREADS = int(os.getenv("DUCKDB_THREADS") or 2)
DUCKDB_MEMORY_LIMIT = os.getenv("DUCKDB_MEMORY_LIMIT") or "1GB"

COMPATIBLE_INPUTS = {
    "data_types": ["plaso:psort:json_line"],
    "mime_types": [],
    "filenames": ["*.jsonl", "*.json_line"],
}

# Task metadata for registration in the core system.
TASK_METADATA = {
    "display_name": "Plaso JSON lines to Parquet",
    "description": (
        "Convert plaso JSON lines (psort -o json_line) to Parquet with DuckDB: typed "
        "columns for the common event fields plus an 'attributes' JSON column."
    ),
    "task_config": [
        {
            "name": "compression",
            "label": "Parquet compression",
            "description": f"One of {', '.join(VALID_COMPRESSION)}. Default: zstd.",
            "type": "select",
            "items": list(VALID_COMPRESSION),
            "required": False,
        },
        {
            "name": "row_group_size",
            "label": "Row group size",
            "description": "Rows per Parquet row group. Smaller uses less memory. Default: 16384.",
            "type": "text",
            "required": False,
        },
    ],
}

log_root = Logger()
logger = log_root.get_logger(__name__, get_task_logger(__name__))


@signals.task_prerun.connect
def on_task_prerun(sender, task_id, task, args, kwargs, **_):
    log_root.bind(
        task_id=task_id,
        task_name=task.name,
        worker_name=TASK_METADATA.get("display_name"),
    )


@celery.task(bind=True, name=TASK_NAME, metadata=TASK_METADATA)
def jsonl_to_parquet(
    self,
    pipe_result: str = None,
    input_files: list = None,
    output_path: str = None,
    workflow_id: str = None,
    task_config: dict = None,
) -> str:
    """Convert each JSON lines input file to a Parquet file.

    Args:
        pipe_result: Base64-encoded result from the previous Celery task, if any.
        input_files: List of input file dictionaries (unused if pipe_result exists).
        output_path: Path to the output directory.
        workflow_id: ID of the workflow.
        task_config: User configuration for the task.

    Returns:
        Base64-encoded dictionary containing task results.
    """
    log_root.bind(workflow_id=workflow_id)
    logger.info(f"Starting {TASK_NAME} for workflow {workflow_id}")

    task_config = task_config or {}
    input_files = get_input_files(pipe_result, input_files or [], filter=COMPATIBLE_INPUTS)
    compression = task_config.get("compression") or "zstd"
    row_group_size = int(task_config.get("row_group_size") or 16384)

    telemetry.add_attribute_to_current_span("input_files", input_files)
    telemetry.add_attribute_to_current_span("task_config", task_config)
    telemetry.add_attribute_to_current_span("workflow_id", workflow_id)

    output_files = []
    row_counts = {}
    for i, input_file in enumerate(input_files, start=1):
        display_name = input_file.get("display_name") or os.path.basename(input_file["path"])
        self.send_event(
            "task-progress",
            data={"status": "Converting to Parquet", "progress": f"File {i} of {len(input_files)}"},
        )
        output_file = create_output_file(
            output_path,
            display_name=f"{os.path.splitext(display_name)[0]}.parquet",
            data_type="openrelik:duckdb:parquet",
            original_path=input_file.get("original_path") or input_file.get("path"),
            # Always registered, so the timeline can be queried in the UI's SQL viewer.
            register_in_db=True,
        )
        rows = convert_jsonl_to_parquet(
            input_file["path"],
            output_file.path,
            compression=compression,
            row_group_size=row_group_size,
            threads=DUCKDB_THREADS,
            memory_limit=DUCKDB_MEMORY_LIMIT,
            temp_directory=os.path.join(output_path, ".duckdb_tmp"),
        )
        logger.info(f"Converted {display_name}: {rows} rows")
        row_counts[display_name] = rows
        output_files.append(output_file.to_dict())

    if not output_files:
        logger.info("No JSON lines input files; nothing to convert.")

    return create_task_result(
        output_files=output_files,
        workflow_id=workflow_id,
        command=f"duckdb COPY ... TO parquet (COMPRESSION {compression})",
        meta={"rows": row_counts},
    )


PLASO_TASK_NAME = "openrelik-worker-duckdb.tasks.plaso_to_parquet"

PLASO_COMPATIBLE_INPUTS = {
    "data_types": ["plaso:log2timeline:plaso_storage"],
    "mime_types": [],
    "filenames": ["*.plaso"],
}

PLASO_TASK_METADATA = {
    "display_name": "Plaso storage to Parquet",
    "description": (
        "Convert a .plaso storage file directly to Parquet, without psort: typed columns "
        "for the common event fields plus an 'attributes' JSON column. Much faster than "
        "psort, but 'message' is empty, duplicate events are kept and rows are in "
        "storage order (sort by timestamp when querying)."
    ),
    "task_config": [
        {
            "name": "compression",
            "label": "Parquet compression",
            "description": f"One of {', '.join(VALID_COMPRESSION)}. Default: zstd.",
            "type": "select",
            "items": list(VALID_COMPRESSION),
            "required": False,
        },
    ],
}


@celery.task(bind=True, name=PLASO_TASK_NAME, metadata=PLASO_TASK_METADATA)
def plaso_to_parquet(
    self,
    pipe_result: str = None,
    input_files: list = None,
    output_path: str = None,
    workflow_id: str = None,
    task_config: dict = None,
) -> str:
    """Convert each .plaso input file to a Parquet file.

    Args:
        pipe_result: Base64-encoded result from the previous Celery task, if any.
        input_files: List of input file dictionaries (unused if pipe_result exists).
        output_path: Path to the output directory.
        workflow_id: ID of the workflow.
        task_config: User configuration for the task.

    Returns:
        Base64-encoded dictionary containing task results.
    """
    log_root.bind(workflow_id=workflow_id)
    logger.info(f"Starting {PLASO_TASK_NAME} for workflow {workflow_id}")

    task_config = task_config or {}
    input_files = get_input_files(pipe_result, input_files or [], filter=PLASO_COMPATIBLE_INPUTS)
    compression = task_config.get("compression") or "zstd"

    telemetry.add_attribute_to_current_span("input_files", input_files)
    telemetry.add_attribute_to_current_span("task_config", task_config)
    telemetry.add_attribute_to_current_span("workflow_id", workflow_id)

    output_files = []
    row_counts = {}
    for i, input_file in enumerate(input_files, start=1):
        display_name = input_file.get("display_name") or os.path.basename(input_file["path"])
        self.send_event(
            "task-progress",
            data={"status": "Converting to Parquet", "progress": f"File {i} of {len(input_files)}"},
        )
        output_file = create_output_file(
            output_path,
            display_name=f"{os.path.splitext(display_name)[0]}.parquet",
            data_type="openrelik:duckdb:plaso_parquet",
            original_path=input_file.get("original_path") or input_file.get("path"),
            # Always registered, so the timeline can be queried in the UI's SQL viewer.
            register_in_db=True,
        )
        rows = convert_plaso_to_parquet(input_file["path"], output_file.path, compression=compression)
        logger.info(f"Converted {display_name}: {rows} rows")
        row_counts[display_name] = rows
        output_files.append(output_file.to_dict())

    if not output_files:
        logger.info("No .plaso input files; nothing to convert.")

    return create_task_result(
        output_files=output_files,
        workflow_id=workflow_id,
        command=f"plaso storage to parquet (COMPRESSION {compression})",
        meta={"rows": row_counts},
    )
