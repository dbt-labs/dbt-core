import betterproto
from colorama import Style

from dbt.events.base_types import NoStdOut, BaseEvent, NoFile, Cache
from dbt.events.types import EventBufferFull, MainReportVersion, EmptyLine
from dbt.events.proto_types import EventInfo
from dbt.events.helpers import env_secrets, scrub_secrets
import dbt.flags as flags

from dbt.constants import METADATA_ENV_PREFIX

from dbt.logger import make_log_dir_if_missing, GLOBAL_LOGGER
from datetime import datetime
import json
import io
from io import StringIO, TextIOWrapper
import logbook
import logging
from logging import Logger
import sys
from logging.handlers import RotatingFileHandler
import os
import uuid
import threading
from typing import Optional, Union, Callable, Dict

from collections import deque

LOG_VERSION = 3
EVENT_HISTORY = None

# create the global file logger with no configuration
FILE_LOG = logging.getLogger("default_file")
null_handler = logging.NullHandler()
FILE_LOG.addHandler(null_handler)

# set up logger to go to stdout with defaults
# setup_event_logger will be called once args have been parsed
STDOUT_LOG = logging.getLogger("default_stdout")
STDOUT_LOG.setLevel(logging.INFO)
stdout_handler = logging.StreamHandler(sys.stdout)
stdout_handler.setLevel(logging.INFO)
STDOUT_LOG.addHandler(stdout_handler)

format_color = True
format_json = False
invocation_id: Optional[str] = None
metadata_vars: Optional[Dict[str, str]] = None


def setup_event_logger(log_path, level_override=None):
    global format_json, format_color, STDOUT_LOG, FILE_LOG
    make_log_dir_if_missing(log_path)

    format_json = flags.LOG_FORMAT == "json"
    # USE_COLORS can be None if the app just started and the cli flags
    # havent been applied yet
    format_color = True if flags.USE_COLORS else False
    # TODO this default should live somewhere better
    log_dest = os.path.join(log_path, "dbt.log")
    level = level_override or (logging.DEBUG if flags.DEBUG else logging.INFO)

    # overwrite the STDOUT_LOG logger with the configured one
    STDOUT_LOG = logging.getLogger("configured_std_out")
    STDOUT_LOG.setLevel(level)

    FORMAT = "%(message)s"
    stdout_passthrough_formatter = logging.Formatter(fmt=FORMAT)

    stdout_handler = logging.StreamHandler(sys.stdout)
    stdout_handler.setFormatter(stdout_passthrough_formatter)
    stdout_handler.setLevel(level)
    # clear existing stdout TextIOWrapper stream handlers
    STDOUT_LOG.handlers = [
        h
        for h in STDOUT_LOG.handlers
        if not (hasattr(h, "stream") and isinstance(h.stream, TextIOWrapper))  # type: ignore
    ]
    STDOUT_LOG.addHandler(stdout_handler)

    # overwrite the FILE_LOG logger with the configured one
    FILE_LOG = logging.getLogger("configured_file")
    FILE_LOG.setLevel(logging.DEBUG)  # always debug regardless of user input

    file_passthrough_formatter = logging.Formatter(fmt=FORMAT)

    file_handler = RotatingFileHandler(
        filename=log_dest, encoding="utf8", maxBytes=10 * 1024 * 1024, backupCount=5  # 10 mb
    )
    file_handler.setFormatter(file_passthrough_formatter)
    file_handler.setLevel(logging.DEBUG)  # always debug regardless of user input
    FILE_LOG.handlers.clear()
    FILE_LOG.addHandler(file_handler)


# used for integration tests
def capture_stdout_logs() -> StringIO:
    global STDOUT_LOG
    capture_buf = io.StringIO()
    stdout_capture_handler = logging.StreamHandler(capture_buf)
    stdout_handler.setLevel(logging.DEBUG)
    STDOUT_LOG.addHandler(stdout_capture_handler)
    return capture_buf


# used for integration tests
def stop_capture_stdout_logs() -> None:
    global STDOUT_LOG
    STDOUT_LOG.handlers = [
        h
        for h in STDOUT_LOG.handlers
        if not (hasattr(h, "stream") and isinstance(h.stream, StringIO))  # type: ignore
    ]


# returns a dictionary representation of the event fields.
# the message may contain secrets which must be scrubbed at the usage site.
def event_to_json(
    event: BaseEvent,
) -> str:
    event_dict = event_to_dict(event)
    raw_log_line = json.dumps(event_dict, sort_keys=True)
    return raw_log_line


def event_to_dict(event: BaseEvent) -> dict:
    event_dict = dict()
    try:
        # We could use to_json here, but it wouldn't sort the keys.
        # The 'to_json' method just does json.dumps on the dict anyway.
        event_dict = event.to_dict(casing=betterproto.Casing.SNAKE, include_default_values=True)  # type: ignore
    except AttributeError as exc:
        event_type = type(event).__name__
        raise Exception(f"type {event_type} is not serializable. {str(exc)}")
    return event_dict


# translates an Event to a completely formatted text-based log line
# type hinting everything as strings so we don't get any unintentional string conversions via str()
def reset_color() -> str:
    global format_color
    return "" if not format_color else Style.RESET_ALL


def create_info_text_log_line(e: BaseEvent) -> str:
    color_tag: str = reset_color()
    ts: str = get_ts().strftime("%H:%M:%S")  # TODO: get this from the event.ts?
    scrubbed_msg: str = scrub_secrets(e.message(), env_secrets())
    log_line: str = f"{color_tag}{ts}  {scrubbed_msg}"
    return log_line


def create_debug_text_log_line(e: BaseEvent) -> str:
    log_line: str = ""
    # Create a separator if this is the beginning of an invocation
    if type(e) == MainReportVersion:
        separator = 30 * "="
        log_line = f"\n\n{separator} {get_ts()} | {get_invocation_id()} {separator}\n"
    color_tag: str = reset_color()
    ts: str = get_ts().strftime("%H:%M:%S.%f")
    scrubbed_msg: str = scrub_secrets(e.message(), env_secrets())
    # Make the levels all 5 characters so they line up
    level: str = f"{e.log_level():<5}"
    thread = ""
    if threading.current_thread().name:
        thread_name = threading.current_thread().name
        thread_name = thread_name[:10]
        thread_name = thread_name.ljust(10, " ")
        thread = f" [{thread_name}]:"
    log_line = log_line + f"{color_tag}{ts} [{level}]{thread} {scrubbed_msg}"
    return log_line


# translates an Event to a completely formatted json log line
def create_json_log_line(e: BaseEvent) -> Optional[str]:
    if type(e) == EmptyLine:
        return None  # will not be sent to logger
    raw_log_line = event_to_json(e)
    return scrub_secrets(raw_log_line, env_secrets())


# calls create_stdout_text_log_line() or create_json_log_line() according to logger config
def create_log_line(e: BaseEvent, file_output=False) -> Optional[str]:
    global format_json
    if format_json:
        return create_json_log_line(e)  # json output, both console and file
    elif file_output is True or flags.DEBUG:
        return create_debug_text_log_line(e)  # default file output
    else:
        return create_info_text_log_line(e)  # console output


# allows for reuse of this obnoxious if else tree.
# do not use for exceptions, it doesn't pass along exc_info, stack_info, or extra
def send_to_logger(l: Union[Logger, logbook.Logger], level: str, log_line: str):
    if not log_line:
        return
    if level == "test":
        # TODO after implmenting #3977 send to new test level
        l.debug(log_line)
    elif level == "debug":
        l.debug(log_line)
    elif level == "info":
        l.info(log_line)
    elif level == "warn":
        l.warning(log_line)
    elif level == "error":
        l.error(log_line)
    else:
        raise AssertionError(
            f"While attempting to log {log_line}, encountered the unhandled level: {level}"
        )


def warn_or_error(event, node=None):
    if flags.WARN_ERROR:
        from dbt.exceptions import raise_compiler_error

        raise_compiler_error(scrub_secrets(event.info.msg, env_secrets()), node)
    else:
        fire_event(event)


# an alternative to fire_event which only creates and logs the event value
# if the condition is met. Does nothing otherwise.
def fire_event_if(conditional: bool, lazy_e: Callable[[], BaseEvent]) -> None:
    if conditional:
        fire_event(lazy_e())


# top-level method for accessing the new eventing system
# this is where all the side effects happen branched by event type
# (i.e. - mutating the event history, printing to stdout, logging
# to files, etc.)
def fire_event(e: BaseEvent) -> None:
    # skip logs when `--log-cache-events` is not passed
    if isinstance(e, Cache) and not flags.LOG_CACHE_EVENTS:
        return

    add_to_event_history(e)

    # backwards compatibility for plugins that require old logger (dbt-rpc)
    if flags.ENABLE_LEGACY_LOGGER:
        # using Event::message because the legacy logger didn't differentiate messages by
        # destination
        log_line = create_log_line(e)
        if log_line:
            send_to_logger(GLOBAL_LOGGER, level=e.log_level(), log_line=log_line)
        return  # exit the function to avoid using the current logger as well

    # always logs debug level regardless of user input
    if not isinstance(e, NoFile):
        log_line = create_log_line(e, file_output=True)
        # doesn't send exceptions to exception logger
        if log_line:
            send_to_logger(FILE_LOG, level=e.log_level(), log_line=log_line)

    if not isinstance(e, NoStdOut):
        # explicitly checking the debug flag here so that potentially expensive-to-construct
        # log messages are not constructed if debug messages are never shown.
        if e.log_level() == "debug" and not flags.DEBUG:
            return  # eat the message in case it was one of the expensive ones
        if e.log_level() != "error" and flags.QUIET:
            return  # eat all non-exception messages in quiet mode

        log_line = create_log_line(e)
        if log_line:
            send_to_logger(STDOUT_LOG, level=e.log_level(), log_line=log_line)


def get_metadata_vars() -> Dict[str, str]:
    global metadata_vars
    if metadata_vars is None:
        metadata_vars = {
            k[len(METADATA_ENV_PREFIX) :]: v
            for k, v in os.environ.items()
            if k.startswith(METADATA_ENV_PREFIX)
        }
    return metadata_vars


def reset_metadata_vars() -> None:
    global metadata_vars
    metadata_vars = None


def get_invocation_id() -> str:
    global invocation_id
    if invocation_id is None:
        invocation_id = str(uuid.uuid4())
    return invocation_id


def set_invocation_id() -> None:
    # This is primarily for setting the invocation_id for separate
    # commands in the dbt servers. It shouldn't be necessary for the CLI.
    global invocation_id
    invocation_id = str(uuid.uuid4())


# exactly one time stamp per concrete event
def get_ts() -> datetime:
    ts = datetime.utcnow()
    return ts


# preformatted time stamp
def get_ts_rfc3339() -> str:
    ts = get_ts()
    ts_rfc3339 = ts.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    return ts_rfc3339


def add_to_event_history(event):
    if flags.EVENT_BUFFER_SIZE == 0:
        return
    global EVENT_HISTORY
    if EVENT_HISTORY is None:
        reset_event_history()
    EVENT_HISTORY.append(event)
    # We only set the EventBufferFull message for event buffers >= 10,000
    if flags.EVENT_BUFFER_SIZE >= 10000 and len(EVENT_HISTORY) == (flags.EVENT_BUFFER_SIZE - 1):
        fire_event(EventBufferFull())


def reset_event_history():
    global EVENT_HISTORY
    EVENT_HISTORY = deque(maxlen=flags.EVENT_BUFFER_SIZE)


# Currently used to set the level in EventInfo, so logging events can
# provide more than one "level". Might be used in the future to set
# more fields in EventInfo, once some of that information is no longer global
def info(level="info"):
    info = EventInfo(level=level)
    return info
