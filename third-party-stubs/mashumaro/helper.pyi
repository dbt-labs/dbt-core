from mashumaro.types import SerializationStrategy
from typing import Any, Callable, Optional, Union
from typing_extensions import Literal

def field_options(
    serialize: Optional[Callable[[Any], Any]] = ...,
    deserialize: Optional[Callable[[Any], Any]] = ...,
    serialization_strategy: Optional[SerializationStrategy] = ...,
    alias: Optional[str] = ...,
): ...

class _PassThrough(SerializationStrategy):
    def __call__(self, *args, **kwargs) -> None: ...
    def serialize(self, value): ...
    def deserialize(self, value): ...

pass_through: Any
