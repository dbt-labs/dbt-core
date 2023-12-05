# Since dbt-rpc does not do its own log setup, and since some events can
# currently fire before logs can be configured by setup_event_logger(), we
# create a default configuration with default settings and no file output.
from dbt.common.events.event_manager import IEventManager, EventManager

_EVENT_MANAGER: IEventManager = EventManager()


def get_event_manager() -> IEventManager:
    global _EVENT_MANAGER
    return _EVENT_MANAGER


def add_logger_to_manager(logger) -> None:
    global _EVENT_MANAGER
    _EVENT_MANAGER.add_logger(logger)


def ctx_set_event_manager(event_manager: IEventManager) -> None:
    global _EVENT_MANAGER
    _EVENT_MANAGER = event_manager


def cleanup_event_logger() -> None:
    # Reset to a no-op manager to release streams associated with logs. This is
    # especially important for tests, since pytest replaces the stdout stream
    # during test runs, and closes the stream after the test is over.
    _EVENT_MANAGER.loggers.clear()
    _EVENT_MANAGER.callbacks.clear()
