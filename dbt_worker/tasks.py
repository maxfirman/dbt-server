import tempfile
from time import sleep, time
from billiard.context import Process
from multiprocessing import Queue

import os
import signal
from fastapi.encoders import jsonable_encoder
from dbt_worker.app import app
from dbt_server.logging import get_configured_celery_logger

from celery.contrib.abortable import AbortableTask
from celery.contrib.abortable import ABORTED
from celery.exceptions import Ignore
from celery.states import PROPAGATE_STATES
from celery.states import FAILURE
from celery.states import STARTED
from celery.states import SUCCESS

from dbt.cli.main import dbtRunner

try:
    from dbt.cli.main import dbtRunnerResult
except (ModuleNotFoundError, ImportError):
    dbtRunnerResult = None
from requests.adapters import HTTPAdapter
from requests import Session
from typing import Any, Dict, Optional, List
from urllib3 import Retry

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


def _send_state_callback(callback_url: str, task_id: str, status: str) -> None:
    """Sends task `status` update callback for `task_id` to `callback_url`."""
    try:
        retries = Retry(total=5, allowed_methods=frozenset(["POST"]))
        session = Session()
        session.mount("http://", HTTPAdapter(max_retries=retries))
        # Existing contract uses status as field name rather than state.
        session.post(callback_url, json={"task_id": task_id, "status": status})
    except Exception as e:
        logger.error(
            f"Send state callback failed, {callback_url}, task_id = {task_id}, status = {status}"
        )
        logger.error(str(e))


def _update_state(
    task: Any,
    task_id: str,
    state: str,
    meta: Dict = None,
    callback_url: Optional[str] = None,
):
    """Updates task state to `state` with `meta` infomation. Triggers callback
    if `callback_url` is set.

    Args:
        task: Celery task object.
        task_id: Which task should be updated. Why do we need this? Because
            celery task.request is a local variable, not shared across thread,
            hence we require task_id to update task state.
        state: Celery worker state.
        callback_url: If set, after state is updated, a callback will be
            triggered."""

    task.update_state(task_id=task_id, state=state, meta=meta)
    if callback_url:
        _send_state_callback(callback_url, task_id, state)


def _invoke_runner(
    task: Any,
    task_id: str,
    command: List[str],
    root_path: str,
    callback_url: Optional[str],
    queue: Queue,
):
    """Invokes dbt runner with `command`, update task state if any exception is
    raised.

    Args:
        task: Celery task.
        task_id: Task id, it's required to update task state.
        command: Dbt invocation command list.
        callback_url: If set, if core raises any error, a callback will be
            triggered.
        queue: Used to send the result back to the main process."""

    # Currently dbt-core does two things that necessitate this chdir logic:
    # 1. If a command is run before deps, a dbt_packages folder is created wherever
    #  the command is being run from
    # 2. On deps, core calls chdir into the project directory and does not reset
    # As a result, we see a dbt_packages dir created at the server root and/or
    # task artifacts being written relative to the project instead of the server
    # after a deps is called. Once core completes the following ticket, we can remove
    # this chdir hack: https://github.com/dbt-labs/dbt-core/issues/6985
    original_wd = os.getcwd()
    try:
        from dbt_server.services import filesystem_service

        dbt = dbtRunner()
        with tempfile.TemporaryDirectory() as tmp_dir:
            filesystem_service.download(root_path, tmp_dir)
            log_path = os.path.join(tmp_dir, task_id)
            command = [LOG_PATH_ARGS, log_path] + command + [PROJECT_DIR_ARGS, tmp_dir]
            result = dbt.invoke(command)
            filesystem_service.upload(log_path, filesystem_service.get_working_dir())
        # dbt-core 1.5.0-latest changes the return type from a tuple to a
        #  dbtRunnerResult obj and no longer raises exceptions on invoke
        if result and type(result) == dbtRunnerResult and not result.success:
            # If task had unhandled errors, raise them
            if result.exception:
                raise result.exception
            else:
                _update_state(
                    task,
                    task_id,
                    FAILURE,
                    None,
                    callback_url,
                )
        serialised_result = jsonable_encoder(result)
        queue.put(serialised_result)
        logger.info(f"Task with id: {task_id} has completed")
    except Exception as e:
        logger.exception(e)
        _update_state(
            task,
            task_id,
            FAILURE,
            {"exc_type": type(e).__name__, "exc_message": str(e)},
            callback_url,
        )

    finally:
        os.chdir(original_wd)


def _get_task_status(task: Any, task_id: str):
    """Retrieves task state for `task_id` from task backend."""
    return task.AsyncResult(task_id).state


def is_command_has_project_dir(command: List[str]) -> bool:
    """Returns true if command has --project-dir args."""
    # This approach is not 100% accurate but should be good for most cases.
    return any([PROJECT_DIR_ARGS in item for item in command])


def raise_exception(*_):
    raise KeyboardInterrupt


def _invoke(
    task: Any,
    command: List[str],
    root_path: str,
    callback_url: Optional[str] = None,
):
    """Invokes dbt command.
    Args:
        command: Dbt commands that will be executed, e.g. ["run",
            "--project-dir", "/a/b/jaffle_shop"].
        callback_url: String, if set any time the task status is updated, worker
            will make a callback. Notice it's not complete, in some cases task
            status may be updated but we are not able to trigger callback, e.g.
            worker process is killed."""
    task_id = task.request.id

    # Make sure celery doesn't ignore sigint, re-raise to allow task cancellation
    signal.signal(signal.SIGINT, raise_exception)

    logger.info(f"Running dbt task ({task_id}) with {command}")
    if callback_url:
        _send_state_callback(callback_url, task_id, STARTED)

    # To support abort, we need to run dbt in a child thread, make parent thread
    # monitor abort signal and join with child thread.
    queue = Queue(maxsize=1)
    p = Process(
        target=_invoke_runner,
        args=[task, task_id, command, root_path, callback_url, queue],
    )
    p.start()
    while p.is_alive():
        sleep(0.5)
        if task.is_aborted():
            _handle_abort(task_id, p, callback_url)

    # By the end of execution, a task state might be.
    # - STARTED, everything is fine!
    # - FAILURE, error occurs.
    # - ABORTED, user abort the task.
    task_status = _get_task_status(task, task_id)
    if task_status == ABORTED:
        _handle_abort(task_id, p, callback_url)

    # If task status is not propagatable, we need to mark it as success manually
    # to trigger callback.
    elif task_status not in PROPAGATE_STATES:
        result = queue.get()
        _update_state(task, task_id, SUCCESS, result, callback_url)

    # Raises Ignore exception to make Celery not automatically set state to
    # SUCCESS.
    raise Ignore()


def _handle_abort(task_id, p, callback_url):
    # Process is not alive, simply return.
    if not p.is_alive():
        return

    # As of 05/2023 there is a bug in dbt-core that will cause occasional
    # timeouts for KeyboardInterrupts.
    # To mediate, wait for process to exit or retry killing it with SIGINT
    retry_timeout = 5  # seconds
    start_time = time()
    while p.is_alive():
        if time() - start_time >= retry_timeout:
            break
        try:
            os.kill(p.pid, signal.SIGINT)
        except Exception as e:
            logger.info(str(e))
        sleep(1)

    # If the process is still alive, send a SIGKILL signal to force it to exit.
    if p.is_alive():
        os.kill(p.pid, signal.SIGKILL)
    try:
        if callback_url:
            _send_state_callback(callback_url, task_id, ABORTED)
    except Exception as e:
        logger.info(str(e))


@app.task(bind=True, track_started=True, base=AbortableTask)
def invoke(
    self,
    command: List[str],
    root_path: str,
    callback_url: Optional[str] = None,
):
    _invoke(self, command, root_path, callback_url)
