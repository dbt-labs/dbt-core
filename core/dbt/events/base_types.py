# Aliasing common Level classes in order to make custom, but not overly-verbose versions that have PROTO_TYPES_MODULE set to the core-specific generated types_pb2 module
from dbt.events import core_types_pb2
from dbt_common.events.base_types import BaseEvent
from dbt_common.events.base_types import DebugLevel as CommonDebugLevel
from dbt_common.events.base_types import DynamicLevel as CommonDyanicLevel
from dbt_common.events.base_types import ErrorLevel as CommonErrorLevel
from dbt_common.events.base_types import InfoLevel as CommonInfoLevel
from dbt_common.events.base_types import TestLevel as CommonTestLevel
from dbt_common.events.base_types import WarnLevel as CommonWarnLevel


class CoreBaseEvent(BaseEvent):
    PROTO_TYPES_MODULE = core_types_pb2


class DynamicLevel(CommonDyanicLevel, CoreBaseEvent):
    pass


class TestLevel(CommonTestLevel, CoreBaseEvent):
    pass


class DebugLevel(CommonDebugLevel, CoreBaseEvent):
    pass


class InfoLevel(CommonInfoLevel, CoreBaseEvent):
    pass


class WarnLevel(CommonWarnLevel, CoreBaseEvent):
    pass


class ErrorLevel(CommonErrorLevel, CoreBaseEvent):
    pass
