
import dbt.logger as logger  # type: ignore # TODO eventually remove dependency on this logger
from dbt.events.history import EVENT_HISTORY
from dbt.events.types import CliEventABC, Event, ShowException
import dbt.flags as flags
import os
from typing import Generator, List


def env_secrets() -> List[str]:
    return [
        v for k, v in os.environ.items()
        if k.startswith(logger.SECRET_ENV_PREFIX)
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
    if isinstance(e, CliEventABC):
        msg = gen_msg(e)
        if e.level_tag() == 'test' and not isinstance(e, ShowException):
            # TODO after implmenting #3977 send to new test level
            logger.GLOBAL_LOGGER.debug(next(msg))
        elif e.level_tag() == 'test' and isinstance(e, ShowException):
            # TODO after implmenting #3977 send to new test level
            logger.GLOBAL_LOGGER.debug(
                next(msg),
                exc_info=e.exc_info,
                stack_info=e.stack_info,
                extra=e.extra
            )
        # explicitly checking the debug flag here so that potentially expensive-to-construct
        # log messages are not constructed if debug messages are never shown.
        elif e.level_tag() == 'debug' and not flags.DEBUG:
            return  # eat the message in case it was one of the expensive ones
        elif e.level_tag() == 'debug' and not isinstance(e, ShowException):
            logger.GLOBAL_LOGGER.debug(next(msg))
        elif e.level_tag() == 'debug' and isinstance(e, ShowException):
            logger.GLOBAL_LOGGER.debug(
                next(msg),
                exc_info=e.exc_info,
                stack_info=e.stack_info,
                extra=e.extra
            )
        elif e.level_tag() == 'info' and not isinstance(e, ShowException):
            logger.GLOBAL_LOGGER.info(next(msg))
        elif e.level_tag() == 'info' and isinstance(e, ShowException):
            logger.GLOBAL_LOGGER.info(
                next(msg),
                exc_info=e.exc_info,
                stack_info=e.stack_info,
                extra=e.extra
            )
        elif e.level_tag() == 'warn' and not isinstance(e, ShowException):
            logger.GLOBAL_LOGGER.warning(next(msg))
        elif e.level_tag() == 'warn' and isinstance(e, ShowException):
            logger.GLOBAL_LOGGER.warning(
                next(msg),
                exc_info=e.exc_info,
                stack_info=e.stack_info,
                extra=e.extra
            )
        elif e.level_tag() == 'error' and not isinstance(e, ShowException):
            logger.GLOBAL_LOGGER.error(next(msg))
        elif e.level_tag() == 'error' and isinstance(e, ShowException):
            logger.GLOBAL_LOGGER.error(
                next(msg),
                exc_info=e.exc_info,
                stack_info=e.stack_info,
                extra=e.extra
            )
        else:
            raise AssertionError(
                f"Event type {type(e).__name__} has unhandled level: {e.level_tag()}"
            )
