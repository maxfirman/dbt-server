import os
import tempfile
from typing import Any, Optional, List

from dbt.cli.main import dbtRunner
from fastapi.encoders import jsonable_encoder

from dbt_server.logging import get_configured_celery_logger
from dbt_worker.app import app

# How long the timeout that parent thread should join with child dbt invocation
# thread. It's used to poll abort status.
LOG_PATH_ARGS = "--log-path"
LOG_FORMAT_ARGS = "--log-format"
LOG_FORMAT_DEFAULT = "json"
PROJECT_DIR_ARGS = "--project-dir"

logger = get_configured_celery_logger()


def is_command_has_log_path(command: List[str]):
    """Returns true if command has --log-path args."""
    # This approach is not 100% accurate but should be good for most cases.
    return any([LOG_PATH_ARGS in item for item in command])


def is_command_has_project_dir(command: List[str]) -> bool:
    """Returns true if command has --project-dir args."""
    # This approach is not 100% accurate but should be good for most cases.
    return any([PROJECT_DIR_ARGS in item for item in command])


def _invoke(
    task: Any,
    command: List[str],
    root_path: str,
):
    task_id = task.request.id

    logger.info(f"Running dbt task ({task_id}) with {command}")

    from dbt_server.services import filesystem_service

    dbt = dbtRunner()
    with tempfile.TemporaryDirectory() as tmp_dir:
        logger.info(f"Downloading dbt definitions for task: {task_id}")
        filesystem_service.download(root_path, tmp_dir)
        log_path = os.path.join(tmp_dir, task_id)

        logger.info(f"Executing dbt.invoke for task: {task_id}")
        result = dbt.invoke(args=command, project_dir=tmp_dir, log_path=log_path)

        logger.info(f"Uploading logs for task: {task_id}")
        filesystem_service.upload(log_path, filesystem_service.get_working_dir())

    logger.debug(f"Serialising result for task: {task_id}")
    serialised_result = jsonable_encoder(result)

    logger.info(f"Task with id: {task_id} has completed")
    return serialised_result


@app.task(bind=True, track_started=True)
def invoke(
    self,
    command: List[str],
    root_path: str,
    callback_url: Optional[str] = None,
):
    return _invoke(self, command, root_path)
