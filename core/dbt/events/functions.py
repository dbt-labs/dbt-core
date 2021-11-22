
from colorama import Style
import dbt.events.functions as this  # don't worry I hate it too.
from dbt.events.base_types import Cli, Event, File, ShowException
from dbt.events.types import T_Event
import dbt.flags as flags
# TODO this will need to move eventually
from dbt.logger import SECRET_ENV_PREFIX, make_log_dir_if_missing, GLOBAL_LOGGER
import io
from io import StringIO, TextIOWrapper
import json
import logbook
import logging
from logging import Logger
from logging.handlers import RotatingFileHandler
import numbers
import os
from typing import Any, Callable, Dict, List, Optional, Set, Tuple, Union
from dataclasses import _FIELD_BASE  # type: ignore[attr-defined]


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
stdout_handler = logging.StreamHandler()
stdout_handler.setLevel(logging.INFO)
STDOUT_LOG.addHandler(stdout_handler)

format_color = True
format_json = False


def setup_event_logger(log_path):
    make_log_dir_if_missing(log_path)
    this.format_json = flags.LOG_FORMAT == 'json'
    # USE_COLORS can be None if the app just started and the cli flags
    # havent been applied yet
    this.format_color = True if flags.USE_COLORS else False
    # TODO this default should live somewhere better
    log_dest = os.path.join(log_path, 'dbt.log')
    level = logging.DEBUG if flags.DEBUG else logging.INFO

    # overwrite the STDOUT_LOG logger with the configured one
    this.STDOUT_LOG = logging.getLogger('configured_std_out')
    this.STDOUT_LOG.setLevel(level)

    FORMAT = "%(message)s"
    stdout_passthrough_formatter = logging.Formatter(fmt=FORMAT)

    stdout_handler = logging.StreamHandler()
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


def scrub_collection_secrets(values: Union[List[Any], Tuple[Any, ...], Set[Any]]):
    for val in values:
        if isinstance(val, str):
            val = scrub_secrets(val, env_secrets())
        elif isinstance(val, numbers.Number):
            continue
        elif isinstance(val, (list, tuple, set)):
            val = scrub_collection_secrets(val)
        elif isinstance(val, dict):
            val = scrub_dict_secrets(val)
    return values


def scrub_dict_secrets(values: Dict) -> Dict:
    scrubbed_values = values
    for key, val in values.items():
        if isinstance(val, str):
            scrubbed_values[key] = scrub_secrets(val, env_secrets())
        elif isinstance(val, numbers.Number):
            continue
        elif isinstance(val, (list, tuple, set)):
            scrubbed_values[key] = scrub_collection_secrets(val)
        elif isinstance(val, dict):
            scrubbed_values[key] = scrub_dict_secrets(val)
    return scrubbed_values


# returns a dictionary representation of the event fields. You must specify which of the
# available messages you would like to use (i.e. - e.message, e.cli_msg(), e.file_msg())
# used for constructing json formatted events. includes secrets which must be scrubbed at
# the usage site.
def event_to_dict(e: T_Event, msg_fn: Callable[[T_Event], str]) -> dict:
    level = e.level_tag()
    return {
        'log_version': e.log_version,
        'ts': e.get_ts(),
        'pid': e.get_pid(),
        'msg': msg_fn(e),
        'level': level,
        'data': Optional[Dict[str, Any]],
        'event_data_serialized': True
    }


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
# you have to specify which message you want. (i.e. - e.message, e.cli_msg(), e.file_msg())
def create_json_log_line(e: T_Event, msg_fn: Callable[[T_Event], str]) -> str:
    values = event_to_dict(e, lambda x: msg_fn(x))
    values['ts'] = e.get_ts().isoformat()
    if hasattr(e, '__dataclass_fields__'):
        values['data'] = {
            x: getattr(e, x) for x, y
            in e.__dataclass_fields__.items()  # type: ignore[attr-defined]
            if type(y._field_type) == _FIELD_BASE
        }
    else:
        values['data'] = None

    # need to catch if any data is not serializable but still make sure as much of
    # the logs go out as possible
    try:
        log_line = json.dumps(scrub_dict_secrets(values), sort_keys=True)
    except TypeError:
        # the only key currently throwing errors is 'data'.  Expand this list
        # as needed if new issues pop up
        values['data'] = None
        values['event_data_serialized'] = False
        log_line = json.dumps(scrub_dict_secrets(values), sort_keys=True)
    return log_line


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
    # TODO manage history in phase 2:  EVENT_HISTORY.append(e)

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
