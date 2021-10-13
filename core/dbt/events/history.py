from dbt.events.types import Event
from typing import List

# the global history of events for this session
EVENT_HISTORY: List[Event] = []
