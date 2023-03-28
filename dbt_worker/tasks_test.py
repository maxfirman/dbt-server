from celery.exceptions import Ignore
from dbt_worker.tasks import _invoke
from time import sleep
from unittest import TestCase
from unittest.mock import patch
from unittest.mock import MagicMock

TEST_LOG_PATH = "/test_path"
TEST_COMMAND = ["run", "--flag", "test"]
TEST_COMMAND_WITH_LOG_PATH = ["--log-path", "test", "run", "--flag", "test"]
TEST_RESOLVED_COMMAND = ["--log-path", TEST_LOG_PATH, "run", "--flag", "test"]
TEST_TASK_ID = "test_id"
TEST_ERROR_MESSAGE = "test error"


class EmptyClass:
    # Empty class to holder some data class member.
    pass


class MockTask:
    def __init__(self) -> None:
        self.request = EmptyClass()
        self.request.id = TEST_TASK_ID
        self.AsyncResult = MagicMock()
        self.update_state = MagicMock()


def mock_invoke_success(command):
    sleep(1)
    mock_invoke_success.last_command = command
    return None, None


def mock_invoke_failure(command):
    sleep(1)
    mock_invoke_failure.last_command = command
    raise Exception(TEST_ERROR_MESSAGE)


@patch("dbt_worker.tasks.get_task_artifacts_path", return_value=TEST_LOG_PATH)
class TestInvoke(TestCase):
    def setUp(self) -> None:
        self.mock_task = MockTask()
        self.mock_dbt_runner = MagicMock()

    def tearDown(self) -> None:
        mock_invoke_success.last_command = None
        mock_invoke_failure.last_command = None

    @patch("dbt_worker.tasks.dbtRunner")
    def test_success(self, patched_dbt_runner, _):
        patched_dbt_runner.return_value = self.mock_dbt_runner
        self.mock_dbt_runner.invoke = mock_invoke_success
        started_state = EmptyClass()
        started_state.state = "STARTED"
        self.mock_task.AsyncResult.return_value = started_state

        with self.assertRaises(Ignore) as _:
            _invoke(self.mock_task, TEST_COMMAND, None)

        self.assertEqual(mock_invoke_success.last_command, TEST_RESOLVED_COMMAND)
        patched_dbt_runner.assert_called_once_with()
        self.mock_task.AsyncResult.assert_called_once_with(TEST_TASK_ID)
        self.mock_task.update_state.assert_called_once_with(
            task_id=TEST_TASK_ID, state="SUCCESS", meta={}
        )

    @patch("dbt_worker.tasks.dbtRunner")
    def test_ignore_log_path(self, patched_dbt_runner, _):
        patched_dbt_runner.return_value = self.mock_dbt_runner
        self.mock_dbt_runner.invoke = mock_invoke_success
        started_state = EmptyClass()
        started_state.state = "STARTED"
        self.mock_task.AsyncResult.return_value = started_state

        with self.assertRaises(Ignore) as _:
            _invoke(self.mock_task, TEST_COMMAND_WITH_LOG_PATH, None)

        self.assertEqual(mock_invoke_success.last_command, TEST_COMMAND_WITH_LOG_PATH)
        patched_dbt_runner.assert_called_once_with()
        self.mock_task.AsyncResult.assert_called_once_with(TEST_TASK_ID)
        self.mock_task.update_state.assert_called_once_with(
            task_id=TEST_TASK_ID, state="SUCCESS", meta={}
        )

    @patch("dbt_worker.tasks.dbtRunner")
    def test_failure(self, patched_dbt_runner, _):
        patched_dbt_runner.return_value = self.mock_dbt_runner
        self.mock_dbt_runner.invoke = mock_invoke_failure
        started_state = EmptyClass()
        started_state.state = "FAILURE"
        self.mock_task.AsyncResult.return_value = started_state

        with self.assertRaises(Ignore) as _:
            _invoke(self.mock_task, TEST_COMMAND, None)

        self.assertEqual(mock_invoke_failure.last_command, TEST_RESOLVED_COMMAND)
        patched_dbt_runner.assert_called_once_with()
        self.mock_task.update_state.assert_called_once_with(
            task_id=TEST_TASK_ID,
            state="FAILURE",
            meta={"exc_type": "Exception", "exc_message": TEST_ERROR_MESSAGE},
        )