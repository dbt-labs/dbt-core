from dataclasses import dataclass
from enum import Enum
import os
import threading
from datetime import datetime
import dbt.events.proto_types as pt
import sys

if sys.version_info >= (3, 8):
    from typing import Protocol
else:
    from typing_extensions import Protocol

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# These base types define the _required structure_ for the concrete event #
# types defined in types.py                                               #
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #


class Cache:
    # Events with this class will only be logged when the `--log-cache-events` flag is passed
    pass


def get_global_metadata_vars() -> dict:
    from dbt.events.functions import get_metadata_vars

    return get_metadata_vars()


def get_invocation_id() -> str:
    from dbt.events.functions import get_invocation_id

    return get_invocation_id()


# exactly one pid per concrete event
def get_pid() -> int:
    return os.getpid()


# preformatted time stamp
def get_ts_rfc3339() -> str:
    ts = datetime.utcnow()
    ts_rfc3339 = ts.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    return ts_rfc3339


# in theory threads can change so we don't cache them.
def get_thread_name() -> str:
    return threading.current_thread().name


# EventLevel is an Enum, but mixing in the 'str' type is suggested in the Python
# documentation, and provides support for json conversion, which fails otherwise.
class EventLevel(str, Enum):
    DEBUG = "debug"
    TEST = "test"
    INFO = "info"
    WARN = "warn"
    ERROR = "error"


@dataclass
class BaseEvent:
    """BaseEvent for proto message generated python events"""

    #   def __post_init__(self):
    #       super().__post_init__()
    #       if not self.info.level:
    #           self.info.level = self.level_tag()
    #       assert self.info.level in ["info", "warn", "error", "debug", "test"]
    #       if not hasattr(self.info, "msg") or not self.info.msg:
    #           self.info.msg = self.message()
    #       self.info.invocation_id = get_invocation_id()
    #       self.info.extra = get_global_metadata_vars()
    #       self.info.ts = datetime.utcnow()
    #       self.info.pid = get_pid()
    #       self.info.thread = get_thread_name()
    #       self.info.code = self.code()
    #       self.info.name = type(self).__name__

    def level_tag(self) -> EventLevel:
        return EventLevel.DEBUG

    def message(self) -> str:
        raise Exception("message() not implemented for event")

    def code(self) -> str:
        raise Exception("code() not implemented for event")


class EventMsg(Protocol):
    info: pt.EventInfo
    data: BaseEvent


def msg_from_base_event(event: BaseEvent, level: EventLevel = None):

    msg_class_name = f"{type(event).__name__}Msg"
    msg_cls = getattr(pt, msg_class_name)

    # level in EventInfo must be a string, not an EventLevel
    msg_level: str = level.value if level else event.level_tag().value
    assert msg_level is not None
    event_info = pt.EventInfo(
        level=msg_level,
        msg=event.message(),
        invocation_id=get_invocation_id(),
        extra=get_global_metadata_vars(),
        ts=datetime.utcnow(),
        pid=get_pid(),
        thread=get_thread_name(),
        code=event.code(),
        name=type(event).__name__,
    )
    new_event = msg_cls(data=event, info=event_info)
    return new_event


# DynamicLevel requires that the level be supplied on the
# event construction call using the "info" function from functions.py
@dataclass  # type: ignore[misc]
class DynamicLevel(BaseEvent):
    pass


@dataclass
class TestLevel(BaseEvent):
    __test__ = False

    def level_tag(self) -> EventLevel:
        return EventLevel.TEST


@dataclass  # type: ignore[misc]
class DebugLevel(BaseEvent):
    def level_tag(self) -> EventLevel:
        return EventLevel.DEBUG


@dataclass  # type: ignore[misc]
class InfoLevel(BaseEvent):
    def level_tag(self) -> EventLevel:
        return EventLevel.INFO


@dataclass  # type: ignore[misc]
class WarnLevel(BaseEvent):
    def level_tag(self) -> EventLevel:
        return EventLevel.WARN


@dataclass  # type: ignore[misc]
class ErrorLevel(BaseEvent):
    def level_tag(self) -> EventLevel:
        return EventLevel.ERROR


# Included to ensure classes with str-type message members are initialized correctly.
@dataclass  # type: ignore[misc]
class AdapterEventStringFunctor:
    def __post_init__(self):
        super().__post_init__()
        if not isinstance(self.base_msg, str):
            self.base_msg = str(self.base_msg)


@dataclass  # type: ignore[misc]
class EventStringFunctor:
    def __post_init__(self):
        super().__post_init__()
        if not isinstance(self.msg, str):
            self.msg = str(self.msg)


# prevents an event from going to the file
# This should rarely be used in core code. It is currently
# only used in integration tests and for the 'clean' command.
class NoFile:
    pass


# prevents an event from going to stdout
class NoStdOut:
    pass
