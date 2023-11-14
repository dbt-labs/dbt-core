import logging
from typing import Union

from dbt.events.base_types import EventLevel
from dbt.events.types import Note

from dbt.events.eventmgr import IEventManager

_log_level_to_event_level_map = {
    logging.DEBUG: EventLevel.DEBUG,
    logging.INFO: EventLevel.INFO,
    logging.WARN: EventLevel.WARN,
    logging.WARNING: EventLevel.WARN,
    logging.ERROR: EventLevel.ERROR,
    logging.CRITICAL: EventLevel.ERROR,
}


class DbtEventLoggingHandler(logging.Handler):
    """A logging handler that wraps the EventManager
    This allows non-dbt packages to log to the dbt event stream.
    All logs are generated as "Note" events.
    """

    def __init__(self, event_manager: IEventManager, level):
        super().__init__(level)
        self.event_manager = event_manager

    def emit(self, record: logging.LogRecord):
        note = Note(msg=record.getMessage())
        level = _log_level_to_event_level_map[record.levelno]
        self.event_manager.fire_event(e=note, level=level)


def set_package_logging(package_name: str, log_level: Union[str, int], event_mgr: IEventManager):
    """Attach dbt's custom logging handler to the package's logger."""
    log = logging.getLogger(package_name)
    log.setLevel(log_level)
    event_handler = DbtEventLoggingHandler(event_manager=event_mgr, level=log_level)
    log.addHandler(event_handler)
