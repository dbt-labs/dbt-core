import logging
from typing import Union

from dbt.events.base_types import EventLevel
from dbt.events.types import Note

from dbt.events.eventmgr import IEventManager


def log_level_to_event_level(log_level: int) -> EventLevel:
    if log_level == logging.DEBUG:
        return EventLevel.DEBUG
    elif log_level == logging.INFO:
        return EventLevel.INFO
    elif log_level == logging.WARNING:
        return EventLevel.WARN
    elif log_level == logging.ERROR:
        return EventLevel.ERROR
    elif log_level == logging.CRITICAL:
        return EventLevel.ERROR
    else:
        return EventLevel.DEBUG


class DbtEventLoggingHandler(logging.Handler):
    """A logging handler that wraps the EventManager
    This allows non-dbt packages to log to the dbt event stream.
    All logs are generated as "Note" events.
    """

    def __init__(self, event_manager: IEventManager, level=logging.NOTSET):
        super().__init__(level)
        self.event_manager = event_manager

    def emit(self, record: logging.LogRecord):
        note = Note(msg=record.getMessage())
        level = log_level_to_event_level(record.levelno)
        self.event_manager.fire_event(e=note, level=level)


def set_package_logging(package_name: str, log_level: Union[str, int], event_mgr: IEventManager):
    """Attach dbt's custom logging handler to the package's logger."""
    log = logging.getLogger(package_name)
    log.setLevel(log_level)
    event_handler = DbtEventLoggingHandler(event_manager=event_mgr, level=log_level)
    log.addHandler(event_handler)
