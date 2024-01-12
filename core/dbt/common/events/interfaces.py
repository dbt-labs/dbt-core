from typing import Protocol, runtime_checkable


@runtime_checkable
class LoggableDbtObject(Protocol):
    def pluralize(self) -> str:
        ...
