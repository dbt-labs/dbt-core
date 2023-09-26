import traceback
from dataclasses import dataclass
from dbt.events.functions import fire_event, EVENT_MANAGER
from dbt.events.contextvars import get_node_info
from dbt.events.event_handler import set_package_logging
from dbt.events.types import (
    AdapterEventDebug,
    AdapterEventInfo,
    AdapterEventWarning,
    AdapterEventError,
)


# N.B. No guarantees for what type param msg is.
@dataclass
class AdapterLogger:
    name: str

    def debug(self, msg, *args) -> None:
        event = AdapterEventDebug(
            name=self.name, base_msg=str(msg), args=list(args), node_info=get_node_info()
        )
        fire_event(event)

    def info(self, msg, *args) -> None:
        event = AdapterEventInfo(
            name=self.name, base_msg=str(msg), args=list(args), node_info=get_node_info()
        )
        fire_event(event)

    def warning(self, msg, *args) -> None:
        event = AdapterEventWarning(
            name=self.name, base_msg=str(msg), args=list(args), node_info=get_node_info()
        )
        fire_event(event)

    def error(self, msg, *args) -> None:
        event = AdapterEventError(
            name=self.name, base_msg=str(msg), args=list(args), node_info=get_node_info()
        )
        fire_event(event)

    # The default exc_info=True is what makes this method different
    def exception(self, msg, *args) -> None:
        exc_info = str(traceback.format_exc())
        event = AdapterEventError(
            name=self.name,
            base_msg=str(msg),
            args=list(args),
            node_info=get_node_info(),
            exc_info=exc_info,
        )
        fire_event(event)

    def critical(self, msg, *args) -> None:
        event = AdapterEventError(
            name=self.name, base_msg=str(msg), args=list(args), node_info=get_node_info()
        )
        fire_event(event)

    @staticmethod
    def set_adapter_dependency_log_level(package_name, level):
        """By default, dbt suppresses non-dbt package logs. This method allows
        you to set the log level for a specific package.
        """
        set_package_logging(package_name, level, EVENT_MANAGER)
