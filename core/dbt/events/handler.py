import logging

from dbt.events.types import Note

from dbt.events.eventmgr import IEventManager


class DbtLoggingHandler(logging.Handler):
    def __init__(self, event_manager: IEventManager, level=logging.NOTSET):
        super().__init__(level)
        self.event_manager = event_manager

    def emit(self, record: logging.LogRecord):
        note = Note(message=record.getMessage())
        self.event_manager.fire_event(e=note)
