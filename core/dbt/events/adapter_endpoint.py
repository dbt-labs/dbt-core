from dataclasses import dataclass
from dbt.events.functions import fire_event
from dbt.events.types import (
    AdapterEventDebug, AdapterEventInfo, AdapterEventWarning, AdapterEventError
)


@dataclass
class AdapterLogger():
    name: str

    def debug(self, *args, **kwargs):
        event = AdapterEventDebug(self.name, args, kwargs)

        event.exc_info = kwargs.get('exc_info')
        event.stack_info = kwargs.get('stack_info')
        event.extra = kwargs.get('extra')

        fire_event(event)

    def info(self, *args, **kwargs):
        event = AdapterEventInfo(self.name, args, kwargs)

        event.exc_info = kwargs.get('exc_info')
        event.stack_info = kwargs.get('stack_info')
        event.extra = kwargs.get('extra')

        fire_event(event)

    def warning(self, *args, **kwargs):
        event = AdapterEventWarning(self.name, args, kwargs)

        event.exc_info = kwargs.get('exc_info')
        event.stack_info = kwargs.get('stack_info')
        event.extra = kwargs.get('extra')

        fire_event(event)

    def error(self, *args, **kwargs):
        event = AdapterEventError(self.name, args, kwargs)

        event.exc_info = kwargs.get('exc_info')
        event.stack_info = kwargs.get('stack_info')
        event.extra = kwargs.get('extra')

        fire_event(event)

    def exception(self, *args, **kwargs):
        event = AdapterEventError(self.name, args, kwargs)

        # defaulting exc_info=True if it is empty is what makes this method different
        x = kwargs.get('exc_info')
        event.exc_info = x if x else True
        event.stack_info = kwargs.get('stack_info')
        event.extra = kwargs.get('extra')

        fire_event(event)
