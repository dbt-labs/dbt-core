from mashumaro.types import SerializationStrategy
from typing import Any, Callable, Dict, Optional, TypeVar, Union
from typing_extensions import Literal

NamedTupleDeserializationEngine = Literal["as_dict", "as_list"]
DateTimeDeserializationEngine = Literal["ciso8601", "pendulum"]
AnyDeserializationEngine = Literal[NamedTupleDeserializationEngine, DateTimeDeserializationEngine]

NamedTupleSerializationEngine = Literal["as_dict", "as_list"]
AnySerializationEngine = Union[NamedTupleSerializationEngine, OmitSerializationEngine]
OmitSerializationEngine = Literal["omit"]

T = TypeVar("T")

def field_options(
    serialize: Optional[Union[AnySerializationEngine, Callable[[Any], Any]]] = ...,
    deserialize: Optional[Union[AnyDeserializationEngine, Callable[[Any], Any]]] = ...,
    serialization_strategy: Optional[SerializationStrategy] = ...,
    alias: Optional[str] = ...,
) -> Dict[str, Any]: ...

class _PassThrough(SerializationStrategy):
    def __call__(self, *args: Any, **kwargs: Any) -> Any: ...
    def serialize(self, value: T) -> T: ...
    def deserialize(self, value: T) -> T: ...

pass_through: Any
