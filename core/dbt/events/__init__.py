from typing import Any, Dict, Set

import dbt.adapters.events.adapter_types_pb2 as adapter_types_pb2
import dbt.adapters.events.types as adapter_dbt_event_types
import dbt.events.core_types_pb2 as core_dbt_event_types_pb2
import dbt.events.types as core_dbt_event_types
import dbt_common.events.types as dbt_event_types
import dbt_common.events.types_pb2 as dbt_event_types_pb2

ALL_EVENT_TYPES: Dict[str, Any] = {
    **dbt_event_types.__dict__,
    **core_dbt_event_types.__dict__,
    **adapter_dbt_event_types.__dict__,
}

ALL_EVENT_NAMES: Set[str] = set(
    [name for name, cls in ALL_EVENT_TYPES.items() if isinstance(cls, type)]
)


ALL_PROTO_TYPES: Dict[str, Any] = {
    **dbt_event_types_pb2.__dict__,
    **core_dbt_event_types_pb2.__dict__,
    **adapter_types_pb2.__dict__,
}
