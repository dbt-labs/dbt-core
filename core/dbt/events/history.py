from dbt.events.base_types import Event
from typing import List


# the global history of events for this session
# TODO this is naive and the memory footprint is likely far too large.
EVENT_HISTORY: List[Event] = []
