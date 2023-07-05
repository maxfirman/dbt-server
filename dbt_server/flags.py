from os import environ
from typing import Optional


class InMemoryFlag:
    """In-memory flags are initialized from environment variables and stored in
    memory."""

    def __init__(self, env_var: Optional[str], default_value: Optional[str]):
        """Initializes in-memory flag from `env_var`. If `env_var` doesn't
        exist fallback to `default_value`.

        Args:
            env_var: environment variable name, e.g. TEST_ENV.
                If it's None or not existed in system environment variable list,
                we will fallback to `default_value`.
            default_value: when `env_var` is None or doesn't exist, we will
                fallback to default_value.
        """
        self.default_value = default_value
        self.env_var = env_var
        self.value = (
            environ[self.env_var]
            if self.env_var is not None and self.env_var in environ
            else self.default_value
        )

    def get(self) -> str:
        """Returns flag value."""
        return self.value

    def set(self, new_value: str) -> None:
        """Sets flag value to `new_value`."""
        self.value = new_value

    def clear(self) -> None:
        """Sets flag value to default value, i.e. remove environment variable
        side effect."""
        self.value = self.default_value


class EnvironFlag:
    """EnvironFlag flags are wrapper to system environment variables. The value
    storage is system environment variable list and any update will reflect to
    system environment variable.
    """

    def __init__(self, env_var: str):
        self.env_var = env_var

    def get(self) -> str:
        """Returns flag value, if it's not found in system environment variable
        list, return None."""
        return environ[self.env_var] if self.env_var in environ else None

    def set(self, new_value: str) -> None:
        """Sets system environment variable to `new_value`."""
        environ[self.env_var] = new_value

    def clear(self) -> None:
        """Removes system environment variable from list. It's no-op if it's not
        in system environment variable list."""
        if self.env_var in environ:
            del environ[self.env_var]


#
# InMemoryFlag.
#

# Identifies which dbt cloud account the dbt-server serves for.
ACCOUNT_ID = InMemoryFlag("ACCOUNT_ID", None)
# Identifies which dbt cloud environment the dbt-server serves
# for.
ENVIRONMENT_ID = InMemoryFlag("ENVIRONMENT_ID", None)
# Workspace id is generated by runtime gateway to identify workspace pod.
WORKSPACE_ID = InMemoryFlag("WORKSPACE_ID", None)
# Identifies the working environment name, e.g. "dev".
APPLICATION_ENVIRONMENT = InMemoryFlag("APPLICATION_ENVIRONMENT", None)
# Target name override used to load local cached manifest.
DBT_TARGET_NAME = InMemoryFlag("__DBT_TARGET_NAME", None)
# Dbt server working directory, it stores local runtime files.
DBT_WORKING_DIR = InMemoryFlag("__DBT_WORKING_DIR", "./working-dir")
# Instructs if dbt server should ignore a first SIGINT or SIGTERM and enable a
# `/shutdown` endpoint.
ALLOW_ORCHESTRATED_SHUTDOWN = InMemoryFlag("ALLOW_ORCHESTRATED_SHUTDOWN", "0")
# Default dbt project directory. It's used to determine source code location.
DBT_PROJECT_DIRECTORY = InMemoryFlag("DBT_PROJECT_DIRECTORY", None)
# Workspace ID for current environment if any-- temporary to differentiate semantic layer usage
WORKSPACE_ID = InMemoryFlag("WORKSPACE_ID", None)

# Task queue configs.

# Celery task broker url for dbt server to send task.
CELERY_BROKER_URL = InMemoryFlag("CELERY_BROKER_URL", "redis://localhost:6379/0")
# Celery task backend url for dbt server to get task results.
CELERY_BACKEND_URL = InMemoryFlag("CELERY_BACKEND_URL", "redis://localhost:6379/0")
# File path to use for celery log formatting -- should match what is used for CELERYD_LOG_FILE in celery config
# or what is passed to --log-file in the celery command if running locally
CELERY_LOG_FILE = InMemoryFlag("CELERY_LOG_FILE", "/var/log/celery/celery-all.log")

#
# EnvironFlag.
#

# Shared with dbt core, it identifies the runtime target directory path.
DBT_TARGET_PATH = EnvironFlag("DBT_TARGET_PATH")
# Shared with dbt core, indicates if compile allows introspection.
DBT_ALLOW_INTROSPECTION = EnvironFlag("__DBT_ALLOW_INTROSPECTION")

FSSPEC_PROTOCOL = InMemoryFlag("FSSPEC_PROTOCOL", "file")
S3_BUCKET = EnvironFlag("S3_BUCKET")
