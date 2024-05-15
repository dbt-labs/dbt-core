from copy import deepcopy

from dbt.events.logging import setup_event_logger
from dbt.flags import get_flags
from dbt_common.events.base_types import BaseEvent
from dbt_common.events.event_manager_client import get_event_manager
from dbt_common.events.logger import LoggerConfig
from tests.functional.utils import EventCatcher


class TestSetupEventLogger:
    def test_clears_preexisting_event_manager_state(self, mock_global_event_manager) -> None:
        manager = get_event_manager()
        manager.add_logger(LoggerConfig(name="test_logger"))
        manager.callbacks.append(EventCatcher(BaseEvent).catch)
        assert len(manager.loggers) == 1
        assert len(manager.callbacks) == 1

        flags = deepcopy(get_flags())
        # setting both of these to none guarantees that no logger will be added
        object.__setattr__(flags, "LOG_LEVEL", "none")
        object.__setattr__(flags, "LOG_LEVEL_FILE", "none")

        setup_event_logger(flags=flags)
        assert len(manager.loggers) == 0
        assert len(manager.callbacks) == 0
