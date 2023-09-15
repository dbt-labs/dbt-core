import logging

from dbt.events.base_types import EventLevel
from dbt.events.event_handler import DbtEventLoggingHandler
from dbt.events.eventmgr import TestEventManager


def test_event_logging_handler_emits_records_correctly():
    event_manager = TestEventManager()
    handler = DbtEventLoggingHandler(event_manager=event_manager)
    log = logging.getLogger("test")
    log.setLevel(logging.DEBUG)
    log.addHandler(handler)

    log.debug("test")
    log.info("test")
    log.warning("test")
    log.error("test")
    log.exception("test")
    log.critical("test")
    assert len(event_manager.event_history) == 6
    assert event_manager.event_history[0][1] == EventLevel.DEBUG
    assert event_manager.event_history[1][1] == EventLevel.INFO
    assert event_manager.event_history[2][1] == EventLevel.WARN
    assert event_manager.event_history[3][1] == EventLevel.ERROR
    assert event_manager.event_history[4][1] == EventLevel.ERROR
    assert event_manager.event_history[5][1] == EventLevel.ERROR
