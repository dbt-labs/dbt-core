
from colorama import Style
from dbt.events.history import EVENT_HISTORY
import dbt.events.functions as this  # don't worry I hate it too.
from dbt.events.types import CliEventABC, Event, ShowException
import dbt.flags as flags
# TODO this will need to move eventually
from dbt.logger import SECRET_ENV_PREFIX, make_log_dir_if_missing
import json
import logging
from logging.handlers import WatchedFileHandler
import os
from typing import Generator, List


# create the global file logger with no configuration
global FILE_LOG
FILE_LOG = logging.getLogger('default_file')

# set up logger to go to stdout with defaults
# setup_event_logger will be called once args have been parsed
global STDOUT_LOG
STDOUT_LOG = logging.getLogger('default_stdout')
STDOUT_LOG.setLevel(logging.INFO)
stdout_handler = logging.StreamHandler()
stdout_handler.setLevel(logging.INFO)
STDOUT_LOG.addHandler(stdout_handler)
global color
format_color = True
global json
format_json = False


def setup_event_logger(log_path):
    make_log_dir_if_missing(log_path)
    this.format_json = flags.LOG_FORMAT == 'json'
    # USE_COLORS can be None if the app just started and the cli flags
    # havent been applied yet
    this.format_color = True if flags.USE_COLORS else False
    # TODO this default should live somewhere better
    log_dest = os.path.join('logs', 'dbt.log')
    # TODO log rotation is not handled by WatchedFileHandler
    level = logging.DEBUG if flags.DEBUG else logging.INFO

    # overwrite the global logger with the configured one
    STDOUT_LOG = logging.getLogger('configured_std_out')
    STDOUT_LOG.setLevel(level)

    FORMAT = "%(message)s"
    passthrough_formatter = logging.Formatter(fmt=FORMAT)

    stdout_handler = logging.StreamHandler()
    stdout_handler.setFormatter(passthrough_formatter)
    stdout_handler.setLevel(level)
    STDOUT_LOG.addHandler(stdout_handler)

    FILE_LOG = logging.getLogger('configured_std_out')
    FILE_LOG.setLevel(level)

    file_handler = WatchedFileHandler(filename=log_dest, encoding='utf8')
    file_handler.setFormatter(passthrough_formatter)
    file_handler.setLevel(level)
    FILE_LOG.addHandler(file_handler)


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


# this returns a generator because some log messages are actually expensive
# to build. For example, many debug messages call `dump_graph()` and we
# don't want to do that in the event that those messages are never going to
# be sent to the user because we are only logging info-level events.
def gen_human_msg(e: CliEventABC) -> Generator[str, None, None]:
    msg = None
    if not log_line:
        msg = scrub_secrets(e.cli_msg(), env_secrets())
    while True:
        yield msg


# translates an Event to a completely formatted output string
def gen_msg_text(e: CliEventABC) -> Generator[str, None, None]:
    log_line = None
    if not log_line:
        values: dict = {
            'ts': e.ts.strftime("%H:%M:%S"),
            'pid': e.pid,
            'msg': next(gen_human_msg(e))
        }
        color_tag = '' if this.format_color else Style.RESET_ALL
        log_line = f"{color_tag}{values['ts']} | {values['msg']}"
    while True:
        yield log_line


# translates an Event to a completely formatted json output string
def gen_msg_json(e: CliEventABC) -> Generator[str, None, None]:
    log_line = None
    if not log_line:
        values: dict = {
            'ts': e.ts.isoformat()
            'pid': e.pid
            'msg': next(gen_human_msg(e))
        }
        log_line = json.dumps(values, sort_keys=True)
    while True:
        yield log_line


# allows for resuse of this obnoxious if else tree.
# do not use for exceptions, it doesn't pass along exc_info, stack_info, or extra
def send_to_logger(l: Logger, level_tag: str, log_line: str):
    if e.level_tag == 'test':
        # TODO after implmenting #3977 send to new test level
        l.debug(log_line)
    elif e.level_tag == 'debug':
        l.debug(log_line)
    elif e.level_tag == 'info':
        l.info(log_line)
    elif e.level_tag == 'warn':
        l.warning(log_line)
    elif e.level_tag == 'error':
        l.error(log_line)
    else:
        raise AssertionError(
            f"Event type {type(e).__name__} has unhandled level: {e.level_tag()}"
        )

def send_exc_to_logger(
    l: Logger,
    level_tag: str,
    log_line: str,
    exec_info=True,
    stack_info=False,
    extra=False
):
    if level_tag == 'test':
        # TODO after implmenting #3977 send to new test level
        l.debug(
            log_line,
            exc_info=e.exc_info,
            stack_info=e.stack_info,
            extra=e.extra
        )
    elif level_tag == 'debug':
        l.debug(
            log_line,
            exc_info=e.exc_info,
            stack_info=e.stack_info,
            extra=e.extra
        )
    elif level_tag == 'info':
        l.info(
            log_line,
            exc_info=e.exc_info,
            stack_info=e.stack_info,
            extra=e.extra
        )
    elif level_tag == 'warn':
        l.warning(
            log_line,
            exc_info=e.exc_info,
            stack_info=e.stack_info,
            extra=e.extra
        )
    elif level_tag == 'error':
        l.error(
            log_line,
            exc_info=e.exc_info,
            stack_info=e.stack_info,
            extra=e.extra
        )
    else:
        raise AssertionError(
            f"Event type {type(e).__name__} has unhandled level: {e.level_tag()}"
        )

# top-level method for accessing the new eventing system
# this is where all the side effects happen branched by event type
# (i.e. - mutating the event history, printing to stdout, logging
# to files, etc.)
def fire_event(e: Event) -> None:
    EVENT_HISTORY.append(e)
    # explicitly checking the debug flag here so that potentially expensive-to-construct
    # log messages are not constructed if debug messages are never shown.
    if e.level_tag() == 'debug' and not flags.DEBUG:
        return  # eat the message in case it was one of the expensive ones

    if isinstance(e, FileEventABC):
        log_line = next(gen_msg_json(e)) if this.format_json else next(gen_msg_text)
        # doesn't send exceptions to exception logger
        send_to_logger(FILE_LOG, level_tag=e.level_tag(), log_line=log_line)
    if isinstance(e, CliEventABC):
        log_line = next(gen_msg_text(e))
        if not isinstance(e, ShowException):
            send_to_logger(STDOUT_LOG, level_tag=e.level_tag(), log_line=log_line)
        # CliEventABC and ShowException
        else:
            send_exc_to_logger(
                STDOUT_LOG,
                level_tag=e.level_tag(),
                log_line: str,
                exec_info=True,
                stack_info=False,
                extra=False
            )
