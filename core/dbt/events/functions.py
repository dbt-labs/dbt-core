
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


# set up logger to go to stdout with defaults
# setup_event_logger will be called once args have been parsed
global LOG
LOG = logging.getLogger('default_event_logger')
LOG.setLevel(logging.INFO)
stdout_handler = logging.StreamHandler()
stdout_handler.setLevel(logging.INFO)
LOG.addHandler(stdout_handler)
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
    LOG = logging.getLogger('configured_event_logger')
    LOG.setLevel(level)

    FORMAT = "%(message)s"
    passthrough_formatter = logging.Formatter(fmt=FORMAT)

    stdout_handler = logging.StreamHandler()
    stdout_handler.setFormatter(passthrough_formatter)
    stdout_handler.setLevel(level)
    LOG.addHandler(stdout_handler)

    file_handler = WatchedFileHandler(filename=log_dest, encoding='utf8')
    file_handler.setFormatter(passthrough_formatter)
    file_handler.setLevel(level)
    LOG.addHandler(file_handler)


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


# translates an Event to a completely formatted output string
#
# this returns a generator because some log messages are actually expensive
# to build. For example, many debug messages call `dump_graph()` and we
# don't want to do that in the event that those messages are never going to
# be sent to the user because we are only logging info-level events.
def gen_msg(e: CliEventABC) -> Generator[str, None, None]:
    final_msg = None
    if not final_msg:
        values: dict = {
            'pid': e.pid
        }
        values['msg'] = scrub_secrets(e.cli_msg(), env_secrets())
        if this.format_json:
            values['ts'] = e.ts.isoformat()
            final_msg = json.dumps(values, sort_keys=True)
        else:
            color_tag = '' if this.format_color else Style.RESET_ALL
            values['ts'] = e.ts.strftime("%H:%M:%S")
            final_msg = f"{color_tag}{values['ts']} | {values['msg']}"
    while True:
        yield final_msg


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
    if isinstance(e, CliEventABC):
        msg = gen_msg(e)
        if not isinstance(e, ShowException):
            if e.level_tag() == 'test':
                # TODO after implmenting #3977 send to new test level
                LOG.debug(next(msg))
            elif e.level_tag() == 'debug':
                LOG.debug(next(msg))
            elif e.level_tag() == 'info':
                LOG.info(next(msg))
            elif e.level_tag() == 'warn':
                LOG.warning(next(msg))
            elif e.level_tag() == 'error':
                LOG.error(next(msg))
            else:
                raise AssertionError(
                    f"Event type {type(e).__name__} has unhandled level: {e.level_tag()}"
                )
        # CliEventABC and ShowException
        else:
            if e.level_tag() == 'test':
                # TODO after implmenting #3977 send to new test level
                LOG.debug(
                    next(msg),
                    exc_info=e.exc_info,
                    stack_info=e.stack_info,
                    extra=e.extra
                )
            elif e.level_tag() == 'debug':
                LOG.debug(
                    next(msg),
                    exc_info=e.exc_info,
                    stack_info=e.stack_info,
                    extra=e.extra
                )
            elif e.level_tag() == 'info':
                LOG.info(
                    next(msg),
                    exc_info=e.exc_info,
                    stack_info=e.stack_info,
                    extra=e.extra
                )
            elif e.level_tag() == 'warn':
                LOG.warning(
                    next(msg),
                    exc_info=e.exc_info,
                    stack_info=e.stack_info,
                    extra=e.extra
                )
            elif e.level_tag() == 'error':
                LOG.error(
                    next(msg),
                    exc_info=e.exc_info,
                    stack_info=e.stack_info,
                    extra=e.extra
                )
            else:
                raise AssertionError(
                    f"Event type {type(e).__name__} has unhandled level: {e.level_tag()}"
                )
