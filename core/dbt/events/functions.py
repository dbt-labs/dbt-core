import betterproto
import io
import json
import logging
import os
import sys
import threading
import uuid
from collections import deque
from datetime import datetime
from io import StringIO, TextIOWrapper
from logging import Logger
from logging.handlers import RotatingFileHandler
from typing import Callable, Dict, List, Optional, Union

import dbt.flags as flags
import logbook
from colorama import Style
from dbt.constants import METADATA_ENV_PREFIX, SECRET_ENV_PREFIX
from dbt.events.base_types import BaseEvent, Cache, NoFile, NoStdOut
from dbt.events.types import EmptyLine, EventBufferFull, MainReportVersion
from dbt.logger import make_log_dir_if_missing

# create the module-globals
LOG_VERSION = 2
EVENT_HISTORY = None

DEFAULT_FILE_LOGGER_NAME = "default_file"
FILE_LOG = logging.getLogger(DEFAULT_FILE_LOGGER_NAME)

DEFAULT_STDOUT_LOGGER_NAME = "default_std_out"
STDOUT_LOG = logging.getLogger(DEFAULT_STDOUT_LOGGER_NAME)

invocation_id: Optional[str] = None
metadata_vars: Optional[Dict[str, str]] = None


def setup_event_logger(log_path, log_format, use_colors, debug):
    global FILE_LOG
    global STDOUT_LOG

    make_log_dir_if_missing(log_path)

    # TODO this default should live somewhere better
    log_dest = os.path.join(log_path, "dbt.log")
    level = logging.DEBUG if debug else logging.INFO

    # overwrite the STDOUT_LOG logger with the configured one
    STDOUT_LOG = logging.getLogger("configured_std_out")
    STDOUT_LOG.setLevel(level)
    setattr(STDOUT_LOG, "format_json", log_format == "json")
    setattr(STDOUT_LOG, "format_color", True if use_colors else False)

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
    setattr(FILE_LOG, "format_json", log_format == "json")
    setattr(FILE_LOG, "format_color", True if use_colors else False)

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
    stdout_capture_handler.setLevel(logging.DEBUG)
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


def env_secrets() -> List[str]:
    return [v for k, v in os.environ.items() if k.startswith(SECRET_ENV_PREFIX) and v.strip()]


def scrub_secrets(msg: str, secrets: List[str]) -> str:
    scrubbed = msg

    for secret in secrets:
        scrubbed = scrubbed.replace(secret, "*****")

    return scrubbed


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
    return Style.RESET_ALL if getattr(STDOUT_LOG, "format_color", False) else ""


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
    level: str = f"{e.level_tag():<5}"
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
    global FILE_LOG
    global STDOUT_LOG

    if FILE_LOG.name == DEFAULT_FILE_LOGGER_NAME and STDOUT_LOG.name == DEFAULT_STDOUT_LOGGER_NAME:

        # TODO: This is only necessary because our test framework doesn't correctly set up logging.
        # This code should be moved to the test framework when we do CT-XXX (tix # needed)
        null_handler = logging.NullHandler()
        FILE_LOG.addHandler(null_handler)
        setattr(FILE_LOG, "format_json", False)
        setattr(FILE_LOG, "format_color", False)

        stdout_handler = logging.StreamHandler(sys.stdout)
        stdout_handler.setLevel(logging.INFO)
        STDOUT_LOG.setLevel(logging.INFO)
        STDOUT_LOG.addHandler(stdout_handler)
        setattr(STDOUT_LOG, "format_json", False)
        setattr(STDOUT_LOG, "format_color", False)

    logger = FILE_LOG if file_output else STDOUT_LOG
    if getattr(logger, "format_json"):
        return create_json_log_line(e)  # json output, both console and file
    elif file_output is True or flags.DEBUG:
        return create_debug_text_log_line(e)  # default file output
    else:
        return create_info_text_log_line(e)  # console output


# allows for reuse of this obnoxious if else tree.
# do not use for exceptions, it doesn't pass along exc_info, stack_info, or extra
def send_to_logger(l: Union[Logger, logbook.Logger], level_tag: str, log_line: str):
    if not log_line:
        return
    if level_tag == "test":
        # TODO after implmenting #3977 send to new test level
        l.debug(log_line)
    elif level_tag == "debug":
        l.debug(log_line)
    elif level_tag == "info":
        l.info(log_line)
    elif level_tag == "warn":
        l.warning(log_line)
    elif level_tag == "error":
        l.error(log_line)
    else:
        raise AssertionError(
            f"While attempting to log {log_line}, encountered the unhandled level: {level_tag}"
        )


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

    # always logs debug level regardless of user input
    if not isinstance(e, NoFile):
        log_line = create_log_line(e, file_output=True)
        # doesn't send exceptions to exception logger
        if log_line:
            send_to_logger(FILE_LOG, level_tag=e.level_tag(), log_line=log_line)

    if not isinstance(e, NoStdOut):
        # explicitly checking the debug flag here so that potentially expensive-to-construct
        # log messages are not constructed if debug messages are never shown.
        if e.level_tag() == "debug" and not flags.DEBUG:
            return  # eat the message in case it was one of the expensive ones
        if e.level_tag() != "error" and flags.QUIET:
            return  # eat all non-exception messages in quiet mode

        log_line = create_log_line(e)
        if log_line:
            send_to_logger(STDOUT_LOG, level_tag=e.level_tag(), log_line=log_line)


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
