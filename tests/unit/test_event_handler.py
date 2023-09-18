import logging

from dbt.events.base_types import EventLevel
from dbt.events.event_handler import DbtEventLoggingHandler, set_package_logging
from dbt.events.eventmgr import TestEventManager


def test_event_logging_handler_emits_records_correctly():
    event_manager = TestEventManager()
    handler = DbtEventLoggingHandler(event_manager=event_manager, level=logging.DEBUG)
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


def test_set_package_logging_sets_level_correctly():
    event_manager = TestEventManager()
    log = logging.getLogger("test")
    set_package_logging("test", logging.DEBUG, event_manager)
    log.debug("debug")
    assert len(event_manager.event_history) == 1
    set_package_logging("test", logging.WARN, event_manager)
    log.debug("debug 2")
    assert len(event_manager.event_history) == 1
    log.warning("warning")
    assert len(event_manager.event_history) == 3  # warning logs create two events
