from dataclasses import dataclass
from dbt.events.functions import fire_event
from dbt.events.types import (
    AdapterEventDebug, AdapterEventInfo, AdapterEventWarning, AdapterEventError
)


@dataclass
class AdapterLogger():
    name: str

    def debug(self, msg, *args, exc_info=None, extra=None, stack_info=False):
        event = AdapterEventDebug(name=self.name, base_msg=msg, args=args)

        event.exc_info = exc_info
        event.extra = extra
        event.stack_info = stack_info

        fire_event(event)

    def info(self, msg, *args, exc_info=None, extra=None, stack_info=False):
        event = AdapterEventInfo(name=self.name, base_msg=msg, args=args)

        event.exc_info = exc_info
        event.extra = extra
        event.stack_info = stack_info

        fire_event(event)

    def warning(self, msg, *args, exc_info=None, extra=None, stack_info=False):
        event = AdapterEventWarning(name=self.name, base_msg=msg, args=args)

        event.exc_info = exc_info
        event.extra = extra
        event.stack_info = stack_info

        fire_event(event)

    def error(self, msg, *args, exc_info=None, extra=None, stack_info=False):
        event = AdapterEventError(name=self.name, base_msg=msg, args=args)

        event.exc_info = exc_info
        event.extra = extra
        event.stack_info = stack_info

        fire_event(event)

    # The default exc_info=True is what makes this method different
    def exception(self, msg, *args, exc_info=True, extra=None, stack_info=False):
        event = AdapterEventError(name=self.name, base_msg=msg, args=args)

        event.exc_info = exc_info
        event.extra = extra
        event.stack_info = stack_info

        fire_event(event)

    def critical(self, msg, *args, exc_info=False, extra=None, stack_info=False):
        event = AdapterEventError(name=self.name, base_msg=msg, args=args)

        event.exc_info = exc_info
        event.extra = extra
        event.stack_info = stack_info

        fire_event(event)
