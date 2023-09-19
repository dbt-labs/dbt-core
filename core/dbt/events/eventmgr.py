import os
import traceback
from typing import Callable, List, Optional, Protocol, Tuple
from uuid import uuid4

from dbt.events.base_types import BaseEvent, EventLevel, msg_from_base_event, EventMsg
from dbt.events.logger import LoggerConfig, _Logger, _TextLogger, _JsonLogger, LineFormat


class EventManager:
    def __init__(self) -> None:
        self.loggers: List[_Logger] = []
        self.callbacks: List[Callable[[EventMsg], None]] = []
        self.invocation_id: str = str(uuid4())

    def fire_event(self, e: BaseEvent, level: Optional[EventLevel] = None) -> None:
        msg = msg_from_base_event(e, level=level)

        if os.environ.get("DBT_TEST_BINARY_SERIALIZATION"):
            print(f"--- {msg.info.name}")
            try:
                msg.SerializeToString()
            except Exception as exc:
                raise Exception(
                    f"{msg.info.name} is not serializable to binary. Originating exception: {exc}, {traceback.format_exc()}"
                )

        for logger in self.loggers:
            if logger.filter(msg):  # type: ignore
                logger.write_line(msg)

        for callback in self.callbacks:
            callback(msg)

    def add_logger(self, config: LoggerConfig) -> None:
        logger = (
            _JsonLogger(config) if config.line_format == LineFormat.Json else _TextLogger(config)
        )
        self.loggers.append(logger)

    def flush(self) -> None:
        for logger in self.loggers:
            logger.flush()


class IEventManager(Protocol):
    callbacks: List[Callable[[EventMsg], None]]
    invocation_id: str
    loggers: List[_Logger]

    def fire_event(self, e: BaseEvent, level: Optional[EventLevel] = None) -> None:
        ...

    def add_logger(self, config: LoggerConfig) -> None:
        ...


class TestEventManager(IEventManager):
    def __init__(self) -> None:
        self.event_history: List[Tuple[BaseEvent, Optional[EventLevel]]] = []
        self.loggers = []

    def fire_event(self, e: BaseEvent, level: Optional[EventLevel] = None) -> None:
        self.event_history.append((e, level))

    def add_logger(self, config: LoggerConfig) -> None:
        raise NotImplementedError()
