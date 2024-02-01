# Aliasing common Level classes in order to make custom, but not overly-verbose versions that have PROTO_TYPES_MODULE set to the core-specific generated types_pb2 module
from dbt_common.events.base_types import (
    BaseEvent,
    DynamicLevel as CommonDyanicLevel,
    TestLevel as CommonTestLevel,
    DebugLevel as CommonDebugLevel,
    InfoLevel as CommonInfoLevel,
    WarnLevel as CommonWarnLevel,
    ErrorLevel as CommonErrorLevel,
)
from dbt.events import core_types_pb2


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
