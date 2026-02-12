import json
import logging
import os
import threading
import typing
import urllib.request
from base64 import b64encode
from datetime import datetime, timezone

from pythonjsonlogger import jsonlogger

from .tools import interactive

# Note:
# The environment variable controls exposed to affect the individual loggers should be considered to be beta.
# The ux/api may change in the future.
# At time of writing, the code was written to preserve existing default behavior
# For now, assume this is the environment variable whose usage will remain unchanged and controls output for all
# loggers defined in this file.
LOGGING_ENV_VAR = "FLYTE_SDK_LOGGING_LEVEL"
# The environment variable controls the logging level for the developer logger.
LOGGING_DEV_ENV_VAR = "FLYTE_SDK_DEV_LOGGING_LEVEL"
LOGGING_FMT_ENV_VAR = "FLYTE_SDK_LOGGING_FORMAT"
LOGGING_RICH_FMT_ENV_VAR = "FLYTE_SDK_RICH_TRACEBACKS"

LOGGING_TELEMETRY_ENV_VAR = "FLYTE_TELEMETRY_ENABLED"
CLICKHOUSE_URL_ENV_VAR = "FLYTE_TELEMETRY_CLICKHOUSE_URL"
CLICKHOUSE_USER_ENV_VAR = "FLYTE_TELEMETRY_CLICKHOUSE_USER"
CLICKHOUSE_PASSWORD_ENV_VAR = "FLYTE_TELEMETRY_CLICKHOUSE_PASSWORD"
CLICKHOUSE_DATABASE_ENV_VAR = "FLYTE_TELEMETRY_CLICKHOUSE_DATABASE"
CLICKHOUSE_TABLE_ENV_VAR = "FLYTE_TELEMETRY_CLICKHOUSE_TABLE"

# By default, the root flytekit logger to debug so everything is logged, but enable fine-tuning
logger = logging.getLogger("flytekit")
user_space_logger = logging.getLogger("user_space")
developer_logger = logging.getLogger("developer")
telemetry_logger = logging.getLogger("flytekit.telemetry")

# Stop propagation so that configuration is isolated to this file (so that it doesn't matter what the
# global Python root logger is set to).
logger.propagate = False
telemetry_logger.propagate = False


def set_flytekit_log_properties(
    handler: typing.Optional[logging.Handler] = None,
    filter: typing.Optional[logging.Filter] = None,
    level: typing.Optional[int] = None,
):
    """
    flytekit logger, refers to the framework logger. It is possible to selectively tune the logging for flytekit.

    Sets the flytekit logger to the specified handler, filter, and level. If any of the parameters are None, then
    the corresponding property on the flytekit logger will not be set.

    :param handler: logging.Handler to add to the flytekit logger
    :param filter: logging.Filter to add to the flytekit logger
    :param level: logging level to set the flytekit logger to
    """
    global logger
    if handler is not None:
        logger.handlers.clear()
        logger.addHandler(handler)
    if filter is not None:
        logger.addFilter(filter)
    if level is not None:
        logger.setLevel(level)


def set_user_logger_properties(
    handler: typing.Optional[logging.Handler] = None,
    filter: typing.Optional[logging.Filter] = None,
    level: typing.Optional[int] = None,
):
    """
    user_space logger, refers to the user's logger. It is possible to selectively tune the logging for the user.

    :param handler: logging.Handler to add to the user_space_logger
    :param filter: logging.Filter to add to the user_space_logger
    :param level: logging level to set the user_space_logger to
    """
    global user_space_logger
    if handler is not None:
        user_space_logger.addHandler(handler)
    if filter is not None:
        user_space_logger.addFilter(filter)
    if level is not None:
        user_space_logger.setLevel(level)


def set_developer_properties(
    handler: typing.Optional[logging.Handler] = None,
    filter: typing.Optional[logging.Filter] = None,
    level: typing.Optional[int] = None,
):
    """
    developer logger is only used for debugging. It is possible to selectively tune the logging for the developer.

    :param handler: logging.Handler to add to the user_space_logger
    :param filter: logging.Filter to add to the user_space_logger
    :param level: logging level to set the user_space_logger to
    """
    global developer_logger
    if handler is not None:
        developer_logger.addHandler(handler)
    if filter is not None:
        developer_logger.addFilter(filter)
    if level is not None:
        developer_logger.setLevel(level)


def _get_env_logging_level(default_level: int = logging.WARNING) -> int:
    """
    Returns the logging level set in the environment variable, or logging.WARNING if the environment variable is not
    set.
    """
    return int(os.getenv(LOGGING_ENV_VAR, default_level))


def _get_dev_env_logging_level(default_level: int = logging.INFO) -> int:
    """
    Returns the logging level set in the environment variable, or logging.INFO if the environment variable is not
    set.
    """
    return int(os.getenv(LOGGING_DEV_ENV_VAR, default_level))


def _is_telemetry_enabled() -> bool:
    return os.environ.get(LOGGING_TELEMETRY_ENV_VAR, "1").lower() in ("1", "true", "t")


def initialize_global_loggers():
    """
    Initializes the global loggers to the default configuration.
    """
    _initialize_telemetry_logger()

    # Use Rich logging while running in the local execution or jupyter notebook.
    if (os.getenv("FLYTE_INTERNAL_EXECUTION_ID") is None or interactive.ipython_check()) and is_rich_logging_enabled():
        try:
            upgrade_to_rich_logging()
            return
        except OSError as e:
            logger.warning(f"Failed to initialize rich logging: {e}")
            pass

    handler = logging.StreamHandler()
    handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter(fmt="[%(name)s] %(message)s")
    if os.environ.get(LOGGING_FMT_ENV_VAR, "json") == "json":
        formatter = jsonlogger.JsonFormatter(fmt="%(asctime)s %(name)s %(levelname)s %(message)s")
    handler.setFormatter(formatter)

    set_flytekit_log_properties(handler, None, _get_env_logging_level())
    set_user_logger_properties(handler, None, logging.INFO)
    set_developer_properties(handler, None, _get_dev_env_logging_level())


def is_rich_logging_enabled() -> bool:
    return os.environ.get(LOGGING_RICH_FMT_ENV_VAR) != "0"


def upgrade_to_rich_logging(log_level: typing.Optional[int] = logging.WARNING):
    import click
    from rich.console import Console
    from rich.logging import RichHandler

    import flytekit

    try:
        width = os.get_terminal_size().columns
    except Exception as e:
        logger.debug(f"Failed to get terminal size: {e}")
        width = 80

    handler = RichHandler(
        tracebacks_suppress=[click, flytekit],
        rich_tracebacks=True,
        omit_repeated_times=False,
        show_path=False,
        log_time_format="%H:%M:%S.%f",
        console=Console(width=width),
    )

    formatter = logging.Formatter(fmt="%(filename)s:%(lineno)d - %(message)s")
    handler.setFormatter(formatter)
    set_flytekit_log_properties(handler, None, _get_env_logging_level(default_level=log_level))
    set_user_logger_properties(handler, None, logging.INFO)
    set_developer_properties(handler, None, _get_dev_env_logging_level())


def get_level_from_cli_verbosity(verbosity: int) -> int:
    """
    Converts a verbosity level from the CLI to a logging level.

    :param verbosity: verbosity level from the CLI
    :return: logging level
    """
    if verbosity == 0:
        return _get_env_logging_level(default_level=logging.CRITICAL)
    elif verbosity == 1:
        return logging.WARNING
    elif verbosity == 2:
        return logging.INFO
    else:
        return logging.DEBUG


def is_display_progress_enabled() -> bool:
    return os.getenv(LOGGING_RICH_FMT_ENV_VAR, False)


class ClickHouseTelemetrySink:
    """Inserts each telemetry event as a single row to ClickHouse via HTTP POST.

    Each call to send() fires a background thread that POSTs one JSONEachRow line
    to ClickHouse's HTTP interface. No buffering, no batching — every step is
    immediately visible in ClickHouse.
    """

    def __init__(self):
        self._url = os.environ.get(CLICKHOUSE_URL_ENV_VAR, "")
        self._user = os.environ.get(CLICKHOUSE_USER_ENV_VAR, "default")
        self._password = os.environ.get(CLICKHOUSE_PASSWORD_ENV_VAR, "")
        self._database = os.environ.get(CLICKHOUSE_DATABASE_ENV_VAR, "default")
        self._table = os.environ.get(CLICKHOUSE_TABLE_ENV_VAR, "flytekit_telemetry")
        self._enabled = bool(self._url)

    @property
    def enabled(self) -> bool:
        return self._enabled

    def send(self, event: typing.Dict[str, typing.Any]):
        """Insert a single telemetry row in a background thread."""
        if not self._enabled:
            return
        event["timestamp"] = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        threading.Thread(target=self._post_row, args=(event,), daemon=True).start()

    def _post_row(self, event: typing.Dict[str, typing.Any]):
        try:
            body = json.dumps(event).encode("utf-8")
            query = f"INSERT INTO {self._database}.{self._table} FORMAT JSONEachRow"
            url = f"{self._url}/?query={urllib.request.quote(query)}"
            credentials = b64encode(f"{self._user}:{self._password}".encode()).decode()
            req = urllib.request.Request(
                url,
                data=body,
                headers={
                    "Content-Type": "application/json",
                    "Authorization": f"Basic {credentials}",
                },
                method="POST",
            )
            urllib.request.urlopen(req, timeout=5)
        except Exception:
            pass


_clickhouse_sink: typing.Optional[ClickHouseTelemetrySink] = None


def get_clickhouse_sink() -> typing.Optional[ClickHouseTelemetrySink]:
    return _clickhouse_sink


def _initialize_telemetry_logger():
    global telemetry_logger, _clickhouse_sink
    if not _is_telemetry_enabled():
        telemetry_logger.setLevel(logging.CRITICAL + 1)
        return

    _clickhouse_sink = ClickHouseTelemetrySink()

    if not _clickhouse_sink.enabled:
        telemetry_handler = logging.StreamHandler()
        telemetry_handler.setLevel(logging.INFO)
        telemetry_handler.setFormatter(
            jsonlogger.JsonFormatter(fmt="%(asctime)s %(name)s %(levelname)s %(message)s")
        )
        telemetry_logger.handlers.clear()
        telemetry_logger.addHandler(telemetry_handler)
        telemetry_logger.setLevel(logging.INFO)
    else:
        telemetry_logger.setLevel(logging.CRITICAL + 1)


# Default initialization
initialize_global_loggers()
