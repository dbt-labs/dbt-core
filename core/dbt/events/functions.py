
import dbt.logger as logger  # type: ignore # TODO eventually remove dependency on this logger
from dbt.events.history import EVENT_HISTORY
from dbt.events.types import CliEventABC, Event


# top-level method for accessing the new eventing system
# this is where all the side effects happen branched by event type
# (i.e. - mutating the event history, printing to stdout, logging
# to files, etc.)
def fire_event(e: Event) -> None:
    EVENT_HISTORY.append(e)
    if isinstance(e, CliEventABC):
        # TODO handle log levels
        logger.GLOBAL_LOGGER.info(logger.timestamped_line(e.cli_msg()))
