# Since dbt-rpc does not do its own log setup, and since some events can
# currently fire before logs can be configured by setup_event_logger(), we
# create a default configuration with default settings and no file output.
import uuid

from dbt.common.events.eventmgr import IEventManager, EventManager

_EVENT_MANAGER: IEventManager = EventManager()


def get_event_manager() -> IEventManager:
    return _EVENT_MANAGER


def get_invocation_id() -> str:
    return _EVENT_MANAGER.invocation_id


def set_invocation_id() -> None:
    # This is primarily for setting the invocation_id for separate
    # commands in the dbt servers. It shouldn't be necessary for the CLI.
    _EVENT_MANAGER.invocation_id = str(uuid.uuid4())


def ctx_set_event_manager(event_manager: IEventManager) -> None:
    global _EVENT_MANAGER
    _EVENT_MANAGER = event_manager


def cleanup_event_logger() -> None:
    # Reset to a no-op manager to release streams associated with logs. This is
    # especially important for tests, since pytest replaces the stdout stream
    # during test runs, and closes the stream after the test is over.
    _EVENT_MANAGER.loggers.clear()
    _EVENT_MANAGER.callbacks.clear()
