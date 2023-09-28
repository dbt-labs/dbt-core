from enum import Enum
import os
import threading
from dbt.events import types_pb2
import sys
from google.protobuf.json_format import ParseDict, MessageToDict, MessageToJson
from google.protobuf.message import Message
from dbt.events.helpers import get_json_string_utcnow
from typing import Optional

if sys.version_info >= (3, 8):
    from typing import Protocol
else:
    from typing_extensions import Protocol


# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# These base types define the _required structure_ for the concrete event #
# types defined in types.py                                               #
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #


def get_global_metadata_vars() -> dict:
    from dbt.events.functions import get_metadata_vars

    return get_metadata_vars()


def get_invocation_id() -> str:
    from dbt.events.functions import get_invocation_id

    return get_invocation_id()


# exactly one pid per concrete event
def get_pid() -> int:
    return os.getpid()


# in theory threads can change, so we don't cache them.
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


class BaseEvent:
    """BaseEvent for proto message generated python events"""

    def __init__(self, *args, **kwargs) -> None:
        class_name = type(self).__name__
        msg_cls = getattr(types_pb2, class_name)
        if class_name == "Formatting" and len(args) > 0:
            kwargs["msg"] = args[0]
            args = ()
        assert (
            len(args) == 0
        ), f"[{class_name}] Don't use positional arguments when constructing logging events"
        if "base_msg" in kwargs:
            kwargs["base_msg"] = str(kwargs["base_msg"])
        if "msg" in kwargs:
            kwargs["msg"] = str(kwargs["msg"])
        try:
            self.pb_msg = ParseDict(kwargs, msg_cls())
        except Exception:
            # Imports need to be here to avoid circular imports
            from dbt.events.types import Note
            from dbt.events.functions import fire_event

            error_msg = f"[{class_name}]: Unable to parse dict {kwargs}"
            # If we're testing throw an error so that we notice failures
            if "pytest" in sys.modules:
                raise Exception(error_msg)
            else:
                fire_event(Note(msg=error_msg), level=EventLevel.WARN)
                self.pb_msg = msg_cls()

    def __setattr__(self, key, value):
        if key == "pb_msg":
            super().__setattr__(key, value)
        else:
            super().__getattribute__("pb_msg").__setattr__(key, value)

    def __getattr__(self, key):
        if key == "pb_msg":
            return super().__getattribute__(key)
        else:
            return super().__getattribute__("pb_msg").__getattribute__(key)

    def to_dict(self):
        return MessageToDict(
            self.pb_msg, preserving_proto_field_name=True, including_default_value_fields=True
        )

    def to_json(self) -> str:
        return MessageToJson(
            self.pb_msg,
            preserving_proto_field_name=True,
            including_default_value_fields=True,
            indent=None,
        )

    def level_tag(self) -> EventLevel:
        return EventLevel.DEBUG

    def message(self) -> str:
        raise Exception("message() not implemented for event")

    def code(self) -> str:
        raise Exception("code() not implemented for event")


class EventInfo(Protocol):
    level: str
    name: str
    ts: str
    code: str


class EventMsg(Protocol):
    info: EventInfo
    data: Message


def msg_from_base_event(event: BaseEvent, level: Optional[EventLevel] = None):

    msg_class_name = f"{type(event).__name__}Msg"
    msg_cls = getattr(types_pb2, msg_class_name)

    # level in EventInfo must be a string, not an EventLevel
    msg_level: str = level.value if level else event.level_tag().value
    assert msg_level is not None
    event_info = {
        "level": msg_level,
        "msg": event.message(),
        "invocation_id": get_invocation_id(),
        "extra": get_global_metadata_vars(),
        "ts": get_json_string_utcnow(),
        "pid": get_pid(),
        "thread": get_thread_name(),
        "code": event.code(),
        "name": type(event).__name__,
    }
    new_event = ParseDict({"info": event_info}, msg_cls())
    new_event.data.CopyFrom(event.pb_msg)
    return new_event


# DynamicLevel requires that the level be supplied on the
# event construction call using the "info" function from functions.py
class DynamicLevel(BaseEvent):
    pass


class TestLevel(BaseEvent):
    __test__ = False

    def level_tag(self) -> EventLevel:
        return EventLevel.TEST


class DebugLevel(BaseEvent):
    def level_tag(self) -> EventLevel:
        return EventLevel.DEBUG


class InfoLevel(BaseEvent):
    def level_tag(self) -> EventLevel:
        return EventLevel.INFO


class WarnLevel(BaseEvent):
    def level_tag(self) -> EventLevel:
        return EventLevel.WARN


class ErrorLevel(BaseEvent):
    def level_tag(self) -> EventLevel:
        return EventLevel.ERROR
