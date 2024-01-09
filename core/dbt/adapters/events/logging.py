import traceback
from dataclasses import dataclass

from dbt.adapters.events.types import (
    AdapterEventDebug,
    AdapterEventInfo,
    AdapterEventWarning,
    AdapterEventError,
)
from dbt_common.events import get_event_manager
from dbt_common.events.contextvars import get_node_info
from dbt_common.events.event_handler import set_package_logging
from dbt_common.events.functions import fire_event


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
        set_package_logging(package_name, level, get_event_manager())
