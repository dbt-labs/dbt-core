
from colorama import Style
import dbt.events.functions as this  # don't worry I hate it too.
from dbt.events.types import Cli, Event, File, ShowException
import dbt.flags as flags
# TODO this will need to move eventually
from dbt.logger import SECRET_ENV_PREFIX, make_log_dir_if_missing
import json
import logging
from logging import Logger
from logging.handlers import WatchedFileHandler
import os
from typing import List


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
    level = logging.DEBUG if flags.DEBUG else logging.INFO

    # overwrite the STDOUT_LOG logger with the configured one
    this.STDOUT_LOG = logging.getLogger('configured_std_out')
    this.STDOUT_LOG.setLevel(level)

    FORMAT = "%(message)s"
    stdout_passthrough_formatter = logging.Formatter(fmt=FORMAT)

    stdout_handler = logging.StreamHandler()
    stdout_handler.setFormatter(stdout_passthrough_formatter)
    stdout_handler.setLevel(level)
    this.STDOUT_LOG.addHandler(stdout_handler)

    # overwrite the FILE_LOG logger with the configured one
    this.FILE_LOG = logging.getLogger('configured_file')
    this.FILE_LOG.setLevel(logging.DEBUG)  # always debug regardless of user input

    file_passthrough_formatter = logging.Formatter(fmt=FORMAT)

    # TODO log rotation is not handled by WatchedFileHandler
    file_handler = WatchedFileHandler(filename=log_dest, encoding='utf8')
    file_handler.setFormatter(file_passthrough_formatter)
    file_handler.setLevel(logging.DEBUG)  # always debug regardless of user input
    this.FILE_LOG.addHandler(file_handler)


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


# translates an Event to a completely formatted output log_line
# json=True -> json formatting
# json=False -> text formatting
# cli=True -> cli formatting
# cli=False -> file formatting
def create_log_line(e: Event, json_fmt: bool, cli_dest: bool) -> str:
    level = e.level_tag()
    values: dict = {
        'pid': e.pid,
        'msg': '',
        'level': level if len(level) == 5 else f"{level} "
    }
    if cli_dest and isinstance(e, Cli):
        values['msg'] = scrub_secrets(e.cli_msg(), env_secrets())
    elif not cli_dest and isinstance(e, File):
        values['msg'] = scrub_secrets(e.file_msg(), env_secrets())

    if json_fmt:
        values['ts'] = e.ts.isoformat()
        log_line = json.dumps(values, sort_keys=True)
    else:
        values['ts'] = e.ts.strftime("%H:%M:%S")
        color_tag = '' if this.format_color else Style.RESET_ALL
        log_line = f"{color_tag}{values['ts']} | [ {values['level']} ] | {values['msg']}"

    return log_line


# allows for resuse of this obnoxious if else tree.
# do not use for exceptions, it doesn't pass along exc_info, stack_info, or extra
def send_to_logger(l: Logger, level_tag: str, log_line: str):
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
    # explicitly checking the debug flag here so that potentially expensive-to-construct
    # log messages are not constructed if debug messages are never shown.

    # always logs debug level regardless of user input
    if isinstance(e, File):
        log_line = create_log_line(e, json_fmt=this.format_json, cli_dest=False)
        # doesn't send exceptions to exception logger
        send_to_logger(FILE_LOG, level_tag=e.level_tag(), log_line=log_line)

    if isinstance(e, Cli):
        if e.level_tag() == 'debug' and not flags.DEBUG:
            return  # eat the message in case it was one of the expensive ones
        log_line = create_log_line(e, json_fmt=this.format_json, cli_dest=True)
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
