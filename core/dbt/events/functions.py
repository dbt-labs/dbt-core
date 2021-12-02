
from colorama import Style
from datetime import datetime
import dbt.events.functions as this  # don't worry I hate it too.
from dbt.events.base_types import Cli, Event, File, ShowException, NodeInfo, Cache
from dbt.events.types import EventBufferFull, T_Event
import dbt.flags as flags
# TODO this will need to move eventually
from dbt.logger import SECRET_ENV_PREFIX, make_log_dir_if_missing, GLOBAL_LOGGER
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
from typing import Any, Callable, Dict, List, Optional, Union
import dataclasses
from collections import deque


# create the global event history buffer with a max size of 100k records
# python 3.7 doesn't support type hints on globals, but mypy requires them. hence the ignore.
# TODO: make the maxlen something configurable from the command line via args(?)
global EVENT_HISTORY
EVENT_HISTORY = deque(maxlen=100000)  # type: ignore

# create the global file logger with no configuration
global FILE_LOG
FILE_LOG = logging.getLogger('default_file')
null_handler = logging.NullHandler()
FILE_LOG.addHandler(null_handler)

# set up logger to go to stdout with defaults
# setup_event_logger will be called once args have been parsed
global STDOUT_LOG
STDOUT_LOG = logging.getLogger('default_stdout')
STDOUT_LOG.setLevel(logging.INFO)
stdout_handler = logging.StreamHandler(sys.stdout)
stdout_handler.setLevel(logging.INFO)
STDOUT_LOG.addHandler(stdout_handler)

format_color = True
format_json = False
invocation_id: Optional[str] = None


def setup_event_logger(log_path, level_override=None):
    make_log_dir_if_missing(log_path)
    this.format_json = flags.LOG_FORMAT == 'json'
    # USE_COLORS can be None if the app just started and the cli flags
    # havent been applied yet
    this.format_color = True if flags.USE_COLORS else False
    # TODO this default should live somewhere better
    log_dest = os.path.join(log_path, 'dbt.log')
    level = level_override or (logging.DEBUG if flags.DEBUG else logging.INFO)

    # overwrite the STDOUT_LOG logger with the configured one
    this.STDOUT_LOG = logging.getLogger('configured_std_out')
    this.STDOUT_LOG.setLevel(level)

    FORMAT = "%(message)s"
    stdout_passthrough_formatter = logging.Formatter(fmt=FORMAT)

    stdout_handler = logging.StreamHandler(sys.stdout)
    stdout_handler.setFormatter(stdout_passthrough_formatter)
    stdout_handler.setLevel(level)
    # clear existing stdout TextIOWrapper stream handlers
    this.STDOUT_LOG.handlers = [
        h for h in this.STDOUT_LOG.handlers
        if not (hasattr(h, 'stream') and isinstance(h.stream, TextIOWrapper))  # type: ignore
    ]
    this.STDOUT_LOG.addHandler(stdout_handler)

    # overwrite the FILE_LOG logger with the configured one
    this.FILE_LOG = logging.getLogger('configured_file')
    this.FILE_LOG.setLevel(logging.DEBUG)  # always debug regardless of user input

    file_passthrough_formatter = logging.Formatter(fmt=FORMAT)

    file_handler = RotatingFileHandler(filename=log_dest, encoding='utf8')
    file_handler.setFormatter(file_passthrough_formatter)
    file_handler.setLevel(logging.DEBUG)  # always debug regardless of user input
    this.FILE_LOG.handlers.clear()
    this.FILE_LOG.addHandler(file_handler)


# used for integration tests
def capture_stdout_logs() -> StringIO:
    capture_buf = io.StringIO()
    stdout_capture_handler = logging.StreamHandler(capture_buf)
    stdout_handler.setLevel(logging.DEBUG)
    this.STDOUT_LOG.addHandler(stdout_capture_handler)
    return capture_buf


# used for integration tests
def stop_capture_stdout_logs() -> None:
    this.STDOUT_LOG.handlers = [
        h for h in this.STDOUT_LOG.handlers
        if not (hasattr(h, 'stream') and isinstance(h.stream, StringIO))  # type: ignore
    ]


def env_secrets() -> List[str]:
    return [
        v for k, v in os.environ.items()
        if k.startswith(SECRET_ENV_PREFIX)
    ]


def scrub_secrets(msg: str, secrets: List[str]) -> str:
    scrubbed = msg

    for secret in secrets:
        scrubbed = scrubbed.replace(secret, "*****")

    return scrubbed


# returns a dictionary representation of the event fields. You must specify which of the
# available messages you would like to use (i.e. - e.message, e.cli_msg(), e.file_msg())
# used for constructing json formatted events. includes secrets which must be scrubbed at
# the usage site.
def event_to_serializable_dict(
    e: T_Event, ts_fn: Callable[[datetime], str],
    msg_fn: Callable[[T_Event], str]
) -> Dict[str, Any]:
    data = dict()
    node_info = dict()
    if hasattr(e, '__dataclass_fields__'):
        if isinstance(e, NodeInfo):
            node_info = dataclasses.asdict(e.get_node_info())

        for field, value in dataclasses.asdict(e).items():  # type: ignore[attr-defined]
            if field not in ["code", "report_node_data"]:
                _json_value = e.fields_to_json(value)

                if not isinstance(_json_value, Exception):
                    data[field] = _json_value
                else:
                    data[field] = f"JSON_SERIALIZE_FAILED: {type(value).__name__, 'NA'}"

    event_dict = {
        'type': 'log_line',
        'log_version': e.log_version,
        'ts': ts_fn(e.get_ts()),
        'pid': e.get_pid(),
        'msg': msg_fn(e),
        'level': e.level_tag(),
        'data': data,
        'invocation_id': e.get_invocation_id(),
        'thread_name': e.get_thread_name(),
        'node_info': node_info,
        'code': e.code
    }

    return event_dict


# translates an Event to a completely formatted text-based log line
# you have to specify which message you want. (i.e. - e.message, e.cli_msg(), e.file_msg())
# type hinting everything as strings so we don't get any unintentional string conversions via str()
def create_text_log_line(e: T_Event, msg_fn: Callable[[T_Event], str]) -> str:
    color_tag: str = '' if this.format_color else Style.RESET_ALL
    ts: str = e.get_ts().strftime("%H:%M:%S")
    scrubbed_msg: str = scrub_secrets(msg_fn(e), env_secrets())
    level: str = e.level_tag() if len(e.level_tag()) == 5 else f"{e.level_tag()} "
    log_line: str = f"{color_tag}{ts} | [ {level} ] | {scrubbed_msg}"
    return log_line


# translates an Event to a completely formatted json log line
# you have to specify which message you want. (i.e. - e.message(), e.cli_msg(), e.file_msg())
def create_json_log_line(e: T_Event, msg_fn: Callable[[T_Event], str]) -> str:
    values = event_to_serializable_dict(e, lambda dt: dt.isoformat(), lambda x: msg_fn(x))
    raw_log_line = json.dumps(values, sort_keys=True)
    return scrub_secrets(raw_log_line, env_secrets())


# calls create_text_log_line() or create_json_log_line() according to logger config
def create_log_line(e: T_Event, msg_fn: Callable[[T_Event], str]) -> str:
    return (
        create_json_log_line(e, msg_fn)
        if this.format_json else
        create_text_log_line(e, msg_fn)
    )


# allows for resuse of this obnoxious if else tree.
# do not use for exceptions, it doesn't pass along exc_info, stack_info, or extra
def send_to_logger(l: Union[Logger, logbook.Logger], level_tag: str, log_line: str):
    if level_tag == 'test':
        # TODO after implmenting #3977 send to new test level
        l.debug(log_line)
    elif level_tag == 'debug':
        l.debug(log_line)
    elif level_tag == 'info':
        l.info(log_line)
    elif level_tag == 'warn':
        l.warning(log_line)
    elif level_tag == 'error':
        l.error(log_line)
    else:
        raise AssertionError(
            f"While attempting to log {log_line}, encountered the unhandled level: {level_tag}"
        )


def send_exc_to_logger(
    l: Logger,
    level_tag: str,
    log_line: str,
    exc_info=True,
    stack_info=False,
    extra=False
):
    if level_tag == 'test':
        # TODO after implmenting #3977 send to new test level
        l.debug(
            log_line,
            exc_info=exc_info,
            stack_info=stack_info,
            extra=extra
        )
    elif level_tag == 'debug':
        l.debug(
            log_line,
            exc_info=exc_info,
            stack_info=stack_info,
            extra=extra
        )
    elif level_tag == 'info':
        l.info(
            log_line,
            exc_info=exc_info,
            stack_info=stack_info,
            extra=extra
        )
    elif level_tag == 'warn':
        l.warning(
            log_line,
            exc_info=exc_info,
            stack_info=stack_info,
            extra=extra
        )
    elif level_tag == 'error':
        l.error(
            log_line,
            exc_info=exc_info,
            stack_info=stack_info,
            extra=extra
        )
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
    # if and only if the event history deque will be completely filled by this event
    # fire warning that old events are now being dropped
    global EVENT_HISTORY
    if len(EVENT_HISTORY) == ((EVENT_HISTORY.maxlen or 100000) - 1):
        fire_event(EventBufferFull())

    EVENT_HISTORY.append(e)

    # backwards compatibility for plugins that require old logger (dbt-rpc)
    if flags.ENABLE_LEGACY_LOGGER:
        # using Event::message because the legacy logger didn't differentiate messages by
        # destination
        log_line = create_log_line(e, msg_fn=lambda x: x.message())

        send_to_logger(GLOBAL_LOGGER, e.level_tag(), log_line)
        return  # exit the function to avoid using the current logger as well

    # always logs debug level regardless of user input
    if isinstance(e, File):
        log_line = create_log_line(e, msg_fn=lambda x: x.file_msg())
        # doesn't send exceptions to exception logger
        send_to_logger(FILE_LOG, level_tag=e.level_tag(), log_line=log_line)

    if isinstance(e, Cli):
        # explicitly checking the debug flag here so that potentially expensive-to-construct
        # log messages are not constructed if debug messages are never shown.
        if e.level_tag() == 'debug' and not flags.DEBUG:
            return  # eat the message in case it was one of the expensive ones

        log_line = create_log_line(e, msg_fn=lambda x: x.cli_msg())
        if not isinstance(e, ShowException):
            send_to_logger(STDOUT_LOG, level_tag=e.level_tag(), log_line=log_line)
        # CliEventABC and ShowException
        else:
            send_exc_to_logger(
                STDOUT_LOG,
                level_tag=e.level_tag(),
                log_line=log_line,
                exc_info=e.exc_info,
                stack_info=e.stack_info,
                extra=e.extra
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
