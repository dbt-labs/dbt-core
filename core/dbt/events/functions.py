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
from typing import Any, Dict, List, Optional, Union

import dbt.flags as flags
import logbook
from colorama import Style
from dbt.constants import SECRET_ENV_PREFIX
from dbt.events.base_types import Cache, Event, NoFile, NoStdOut, ShowException
from dbt.events.types import EmptyLine, EventBufferFull, MainReportVersion, T_Event
from dbt.logger import make_log_dir_if_missing

# create the module-globals
LOG_VERSION = 2
EVENT_HISTORY = None

FILE_LOG = logging.getLogger("default_file")
STDOUT_LOG = logging.getLogger("default_std_out")

invocation_id: Optional[str] = None


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
    STDOUT_LOG.format_json = log_format == "json"
    STDOUT_LOG.format_color = True if use_colors else False

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
    FILE_LOG.format_json = log_format == "json"
    FILE_LOG.format_color = True if use_colors else False

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
    capture_buf = io.StringIO()
    stdout_capture_handler = logging.StreamHandler(capture_buf)
    stdout_capture_handler.setLevel(logging.DEBUG)
    STDOUT_LOG.addHandler(stdout_capture_handler)
    return capture_buf


# used for integration tests
def stop_capture_stdout_logs() -> None:
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
def event_to_serializable_dict(
    e: T_Event,
) -> Dict[str, Any]:

    log_line = dict()
    try:
        log_line = e.to_dict()
    except AttributeError as exc:
        event_type = type(e).__name__
        raise Exception(  # TODO this may hang async threads
            f"type {event_type} is not serializable. {str(exc)}"
        )

    # We get the code from the event object, so we don't need it in the data
    if "code" in log_line:
        del log_line["code"]

    event_dict = {
        "type": "log_line",
        "log_version": LOG_VERSION,
        "ts": get_ts_rfc3339(),
        "pid": e.get_pid(),
        "msg": e.message(),
        "level": e.level_tag(),
        "data": log_line,
        "invocation_id": e.get_invocation_id(),
        "thread_name": e.get_thread_name(),
        "code": e.code,
    }

    return event_dict


# translates an Event to a completely formatted text-based log line
# type hinting everything as strings so we don't get any unintentional string conversions via str()
def reset_color() -> str:
    return Style.RESET_ALL if getattr(STDOUT_LOG, "format_color", False) else ""


def create_info_text_log_line(e: T_Event) -> str:
    color_tag: str = reset_color()
    ts: str = get_ts().strftime("%H:%M:%S")
    scrubbed_msg: str = scrub_secrets(e.message(), env_secrets())
    log_line: str = f"{color_tag}{ts}  {scrubbed_msg}"
    return log_line


def create_debug_text_log_line(e: T_Event) -> str:
    log_line: str = ""
    # Create a separator if this is the beginning of an invocation
    if type(e) == MainReportVersion:
        separator = 30 * "="
        log_line = f"\n\n{separator} {get_ts()} | {get_invocation_id()} {separator}\n"
    color_tag: str = reset_color()
    ts: str = get_ts().strftime("%H:%M:%S.%f")
    scrubbed_msg: str = scrub_secrets(e.message(), env_secrets())
    level: str = e.level_tag() if len(e.level_tag()) == 5 else f"{e.level_tag()} "
    thread = ""
    if threading.current_thread().name:
        thread_name = threading.current_thread().name
        thread_name = thread_name[:10]
        thread_name = thread_name.ljust(10, " ")
        thread = f" [{thread_name}]:"
    log_line = log_line + f"{color_tag}{ts} [{level}]{thread} {scrubbed_msg}"
    return log_line


# translates an Event to a completely formatted json log line
def create_json_log_line(e: T_Event) -> Optional[str]:
    if type(e) == EmptyLine:
        return None  # will not be sent to logger
    # using preformatted ts string instead of formatting it here to be extra careful about timezone
    values = event_to_serializable_dict(e)
    raw_log_line = json.dumps(values, sort_keys=True)
    return scrub_secrets(raw_log_line, env_secrets())


# calls create_stdout_text_log_line() or create_json_log_line() according to logger config
def create_log_line(e: T_Event, file_output=False) -> Optional[str]:
    global FILE_LOG
    global STDOUT_LOG
    if FILE_LOG is None and STDOUT_LOG is None:

        # TODO: This is only necessary because our test framework doesn't correctly set up logging.
        # This code should be moved to the test framework when we do CT-XXX (tix # needed)
        null_handler = logging.NullHandler()
        FILE_LOG.addHandler(null_handler)
        FILE_LOG.format_json = False
        FILE_LOG.format_color = False

        stdout_handler = logging.StreamHandler(sys.stdout)
        stdout_handler.setLevel(logging.INFO)
        STDOUT_LOG.setLevel(logging.INFO)
        STDOUT_LOG.addHandler(stdout_handler)
        STDOUT_LOG.format_json = False
        STDOUT_LOG.format_color = False

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


def send_exc_to_logger(
    l: Logger, level_tag: str, log_line: str, exc_info=True, stack_info=False, extra=False
):
    if level_tag == "test":
        # TODO after implmenting #3977 send to new test level
        l.debug(log_line, exc_info=exc_info, stack_info=stack_info, extra=extra)
    elif level_tag == "debug":
        l.debug(log_line, exc_info=exc_info, stack_info=stack_info, extra=extra)
    elif level_tag == "info":
        l.info(log_line, exc_info=exc_info, stack_info=stack_info, extra=extra)
    elif level_tag == "warn":
        l.warning(log_line, exc_info=exc_info, stack_info=stack_info, extra=extra)
    elif level_tag == "error":
        l.error(log_line, exc_info=exc_info, stack_info=stack_info, extra=extra)
    else:
        raise AssertionError(
            f"While attempting to log {log_line}, encountered the unhandled level: {level_tag}"
        )


# top-level method for accessing the new eventing system
# this is where all the side effects happen branched by event type
# (i.e. - mutating the event history, printing to stdout, logging
# to files, etc.)
def fire_event(e: Event) -> None:
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
            if not isinstance(e, ShowException):
                send_to_logger(STDOUT_LOG, level_tag=e.level_tag(), log_line=log_line)
            else:
                send_exc_to_logger(
                    STDOUT_LOG,
                    level_tag=e.level_tag(),
                    log_line=log_line,
                    exc_info=e.exc_info,
                    stack_info=e.stack_info,
                    extra=e.extra,
                )


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
