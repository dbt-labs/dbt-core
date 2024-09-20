from _typeshed import Incomplete
from mashumaro.dialect import Dialect as Dialect
from mashumaro.helper import pass_through as pass_through
from mashumaro.mixins.dict import DataClassDictMixin as DataClassDictMixin
from typing import Any, Callable, Dict, TypeVar, Type

T = TypeVar("T", bound="DataClassMessagePackMixin")
EncodedData = bytes
Encoder = Callable[[Any], EncodedData]
Decoder = Callable[[EncodedData], Dict[Any, Any]]

class MessagePackDialect(Dialect):
    serialization_strategy: Incomplete

def default_encoder(data: Any) -> EncodedData: ...
def default_decoder(data: EncodedData) -> Dict[Any, Any]: ...

class DataClassMessagePackMixin(DataClassDictMixin):
    def to_msgpack(self, encoder: Encoder = ..., **to_dict_kwargs: Any) -> EncodedData: ...
    @classmethod
    def from_msgpack(
        cls: Type[T], data: EncodedData, decoder: Decoder = ..., **from_dict_kwargs: Any
    ) -> T: ...
