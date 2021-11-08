
from dbt.events.history import EVENT_HISTORY
from dbt.events.types import CliEventABC, Event, ShowException
import dbt.flags as flags
from dbt.logger import SECRET_ENV_PREFIX  # TODO this will need to move eventually
import logging
import os
from typing import Generator, List


global LOG
LOG = logging.getLogger()
stdout_handler = logging.StreamHandler()
LOG.addHandler(stdout_handler)


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


# this exists because some log messages are actually expensive to build.
# for example, many debug messages call `dump_graph()` and we don't want to
# do that in the event that those messages are never going to be sent to
# the user because we are only logging info-level events.
def gen_msg(e: CliEventABC) -> Generator[str, None, None]:
    msg = None
    if not msg:
        msg = scrub_secrets(e.cli_msg(), env_secrets())
    while True:
        yield msg


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
