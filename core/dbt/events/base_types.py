from abc import ABCMeta, abstractmethod, abstractproperty
from dataclasses import dataclass
from datetime import datetime
import json
import os
import threading
from typing import Any, Optional

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# These base types define the _required structure_ for the concrete event #
# types defined in types.py                                               #
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #


# in preparation for #3977
class TestLevel():
    def level_tag(self) -> str:
        return "test"


class DebugLevel():
    def level_tag(self) -> str:
        return "debug"


class InfoLevel():
    def level_tag(self) -> str:
        return "info"


class WarnLevel():
    def level_tag(self) -> str:
        return "warn"


class ErrorLevel():
    def level_tag(self) -> str:
        return "error"


class Cache():
    # Events with this class will only be logged when the `--log-cache-events` flag is passed
    pass


@dataclass
class Node():
    node_path: str
    node_name: str
    unique_id: str
    resource_type: str
    materialized: str
    node_status: str
    node_started_at: datetime
    node_finished_at: Optional[datetime]
    type: str = 'node_status'


@dataclass
class ShowException():
    # N.B.:
    # As long as we stick with the current convention of setting the member vars in the
    # `message` method of subclasses, this is a safe operation.
    # If that ever changes we'll want to reassess.
    def __post_init__(self):
        self.exc_info: Any = True
        self.stack_info: Any = None
        self.extra: Any = None


# TODO add exhaustiveness checking for subclasses
# can't use ABCs with @dataclass because of https://github.com/python/mypy/issues/5374
# top-level superclass for all events
class Event(metaclass=ABCMeta):
    # fields that should be on all events with their default implementations
    log_version: int = 1
    ts: Optional[datetime] = None  # use getter for non-optional
    pid: Optional[int] = None  # use getter for non-optional
    node_info: Optional[Node]

    # four digit string code that uniquely identifies this type of event
    # uniqueness and valid characters are enforced by tests
    @abstractproperty
    @staticmethod
    def code() -> str:
        raise Exception("code() not implemented for event")

    # do not define this yourself. inherit it from one of the above level types.
    @abstractmethod
    def level_tag(self) -> str:
        raise Exception("level_tag not implemented for Event")

    # Solely the human readable message. Timestamps and formatting will be added by the logger.
    # Must override yourself
    @abstractmethod
    def message(self) -> str:
        raise Exception("msg not implemented for Event")

    # override this method to convert non-json serializable fields to json.
    # for override examples, see existing concrete types.
    #
    # there is no type-level mechanism to have mypy enforce json serializability, so we just try
    # to serialize and raise an exception at runtime when that fails. This safety mechanism
    # only works if we have attempted to serialize every concrete event type in our tests.
    def fields_to_json(self, field_value: Any) -> Any:
        try:
            json.dumps(field_value, sort_keys=True)
            return field_value
        except TypeError:
            val_type = type(field_value).__name__
            event_type = type(self).__name__
            return Exception(
                f"type {val_type} is not serializable to json."
                f" First make sure that the call sites for {event_type} match the type hints"
                f" and if they do, you can override Event::fields_to_json in {event_type} in"
                " types.py to define your own serialization function to any valid json type"
            )

    # exactly one time stamp per concrete event
    def get_ts(self) -> datetime:
        if not self.ts:
            self.ts = datetime.now()
        return self.ts

    # exactly one pid per concrete event
    def get_pid(self) -> int:
        if not self.pid:
            self.pid = os.getpid()
        return self.pid

    # in theory threads can change so we don't cache them.
    def get_thread_name(self) -> str:
        return threading.current_thread().getName()

    @classmethod
    def get_invocation_id(cls) -> str:
        from dbt.events.functions import get_invocation_id
        return get_invocation_id()


@dataclass  # type: ignore
class NodeInfo(Event, metaclass=ABCMeta):
    report_node_data: Any  # Union[ParsedModelNode, ...] TODO: resolve circular imports

    def get_node_info(self):
        node_info = Node(
            node_path=self.report_node_data.path,
            node_name=self.report_node_data.name,
            unique_id=self.report_node_data.unique_id,
            resource_type=self.report_node_data.resource_type.value,
            materialized=self.report_node_data.config.materialized,
            node_status=str(self.report_node_data._event_status.get('node_status')),
            node_started_at=self.report_node_data._event_status.get("started_at"),
            node_finished_at=self.report_node_data._event_status.get("finished_at")
        )
        return node_info


class File(Event, metaclass=ABCMeta):
    # Solely the human readable message. Timestamps and formatting will be added by the logger.
    def file_msg(self) -> str:
        # returns the event msg unless overriden in the concrete class
        return self.message()


class Cli(Event, metaclass=ABCMeta):
    # Solely the human readable message. Timestamps and formatting will be added by the logger.
    def cli_msg(self) -> str:
        # returns the event msg unless overriden in the concrete class
        return self.message()
