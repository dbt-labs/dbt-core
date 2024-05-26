from dataclasses import dataclass, field
from typing import List

from dbt_common.events.base_types import BaseEvent, EventMsg


@dataclass
class EventCatcher:
    event_to_catch: BaseEvent
    caught_events: List[EventMsg] = field(default_factory=list)

    def catch(self, event: EventMsg):
        if event.info.name == self.event_to_catch.__name__:
            self.caught_events.append(event)

    def flush(self) -> None:
        self.caught_events = []
