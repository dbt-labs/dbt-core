from mashumaro.types import SerializationStrategy
from typing import Any, Callable, Optional, Union
from typing_extensions import Literal

NamedTupleDeserializationEngine = Literal["as_dict", "as_list"]
DateTimeDeserializationEngine = Literal["ciso8601", "pendulum"]
AnyDeserializationEngine = Literal[NamedTupleDeserializationEngine, DateTimeDeserializationEngine]

NamedTupleSerializationEngine = Literal["as_dict", "as_list"]
AnySerializationEngine = NamedTupleSerializationEngine

def field_options(
    serialize: Optional[Union[AnySerializationEngine, Callable[[Any], Any]]] = ...,
    deserialize: Optional[Union[AnyDeserializationEngine, Callable[[Any], Any]]] = ...,
    serialization_strategy: Optional[SerializationStrategy] = ...,
    alias: Optional[str] = ...,
) -> Any: ...

class _PassThrough(SerializationStrategy):
    def __call__(self, *args: Any, **kwargs: Any) -> None: ...
    def serialize(self, value: Any): ...
    def deserialize(self, value: Any): ...

pass_through: Any
