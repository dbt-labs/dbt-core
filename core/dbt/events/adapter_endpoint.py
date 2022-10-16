import traceback
from dataclasses import dataclass
from dbt.events.functions import fire_event
from dbt.events.types import (
    AdapterEventDebug,
    AdapterEventInfo,
    AdapterEventWarning,
    AdapterEventError,
)


@dataclass
class AdapterLogger:
    name: str

    def debug(self, msg, *args):
        event = AdapterEventDebug(name=self.name, base_msg=msg, args=args)
        fire_event(event)

    def info(self, msg, *args):
        event = AdapterEventInfo(name=self.name, base_msg=msg, args=args)
        fire_event(event)

    def warning(self, msg, *args):
        event = AdapterEventWarning(name=self.name, base_msg=msg, args=args)
        fire_event(event)

    def error(self, msg, *args):
        event = AdapterEventError(name=self.name, base_msg=msg, args=args)
        fire_event(event)

    # The default exc_info=True is what makes this method different
    def exception(self, msg, *args):
        event = AdapterEventError(name=self.name, base_msg=msg, args=args)
        event.exc_info = traceback.format_exc()
        fire_event(event)

    def critical(self, msg, *args):
        event = AdapterEventError(name=self.name, base_msg=msg, args=args)
        fire_event(event)
