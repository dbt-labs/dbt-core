
import dbt.logger as logger  # type: ignore # TODO eventually remove dependency on this logger
from dbt.events.history import EVENT_HISTORY
from dbt.events.types import (
    CliEventABC, Event, Level, TestLevel, DebugLevel, InfoLevel, WarnLevel, ErrorLevel
)
from typing import NoReturn


# common trick for getting mypy to do exhaustiveness checks
# will come up with something like `"assert_never" has incompatible type`
# if something is missing.
def assert_never(x: NoReturn) -> NoReturn:
    raise AssertionError("Unhandled type: {}".format(type(x).__name__))


# top-level method for accessing the new eventing system
# this is where all the side effects happen branched by event type
# (i.e. - mutating the event history, printing to stdout, logging
# to files, etc.)
def fire_event(e: Event) -> None:
    EVENT_HISTORY.append(e)
    if isinstance(e, CliEventABC):
        if e.level_tag == 'test':
            # TODO after implmenting #3977 send to new test level
            logger.GLOBAL_LOGGER.debug(logger.timestamped_line(e.cli_msg()))
        elif e.level_tag == 'debug':
            logger.GLOBAL_LOGGER.debug(logger.timestamped_line(e.cli_msg()))
        elif e.level_tag == 'info':
            logger.GLOBAL_LOGGER.info(logger.timestamped_line(e.cli_msg()))
        elif e.level_tag == 'warn':
            logger.GLOBAL_LOGGER.warning()(logger.timestamped_line(e.cli_msg()))
        elif e.level_tag == 'error':
            logger.GLOBAL_LOGGER.error(logger.timestamped_line(e.cli_msg()))
        else:
            raise AssertionError("Event with unhandled level: {}".format(type(e).__name__))
