from abc import ABCMeta, abstractproperty, abstractmethod
from dataclasses import dataclass
from dbt.events.serialization import EventSerialization
import os
import threading
from typing import Any, Dict


# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# These base types define the _required structure_ for the concrete event #
# types defined in types.py                                               #
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #


class Cache:
    # Events with this class will only be logged when the `--log-cache-events` flag is passed
    pass


@dataclass
class ShowException:
    # N.B.:
    # As long as we stick with the current convention of setting the member vars in the
    # `message` method of subclasses, this is a safe operation.
    # If that ever changes we'll want to reassess.
    def __post_init__(self):
        self.exc_info: Any = True
        self.stack_info: Any = None
        self.extra: Any = None


# TODO add exhaustiveness checking for subclasses
# top-level superclass for all events
class Event(metaclass=ABCMeta):
    # Do not define fields with defaults here

    # four digit string code that uniquely identifies this type of event
    # uniqueness and valid characters are enforced by tests
    @abstractproperty
    @staticmethod
    def code() -> str:
        raise Exception("code() not implemented for event")

    # The 'to_dict' method is added by mashumaro via the EventSerialization.
    # It should be in all subclasses that are to record actual events.
    @abstractmethod
    def to_dict(self):
        raise Exception("to_dict not implemented for Event")

    # do not define this yourself. inherit it from one of the above level types.
    @abstractmethod
    def level_tag(self) -> str:
        raise Exception("level_tag not implemented for Event")

    # Solely the human readable message. Timestamps and formatting will be added by the logger.
    # Must override yourself
    @abstractmethod
    def message(self) -> str:
        raise Exception("msg not implemented for Event")

    # exactly one pid per concrete event
    def get_pid(self) -> int:
        return os.getpid()

    # in theory threads can change so we don't cache them.
    def get_thread_name(self) -> str:
        return threading.current_thread().name

    @classmethod
    def get_invocation_id(cls) -> str:
        from dbt.events.functions import get_invocation_id

        return get_invocation_id()


# in preparation for #3977
@dataclass  # type: ignore[misc]
class TestLevel(EventSerialization, Event):
    def level_tag(self) -> str:
        return "test"


@dataclass  # type: ignore[misc]
class DebugLevel(EventSerialization, Event):
    def level_tag(self) -> str:
        return "debug"


@dataclass  # type: ignore[misc]
class InfoLevel(EventSerialization, Event):
    def level_tag(self) -> str:
        return "info"


@dataclass  # type: ignore[misc]
class WarnLevel(EventSerialization, Event):
    def level_tag(self) -> str:
        return "warn"


@dataclass  # type: ignore[misc]
class ErrorLevel(EventSerialization, Event):
    def level_tag(self) -> str:
        return "error"


# prevents an event from going to the file
class NoFile:
    pass


# prevents an event from going to stdout
class NoStdOut:
    pass


# This class represents the node_info which is generated
# by the NodeInfoMixin class in dbt.contracts.graph.parsed
@dataclass
class NodeInfo:
    node_info: Dict[str, Any]
