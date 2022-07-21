from mashumaro.dialects.msgpack import MessagePackDialect as MessagePackDialect
from mashumaro.mixins.dict import DataClassDictMixin as DataClassDictMixin
from typing import Any, Dict, Type, TypeVar
from typing_extensions import Protocol as Protocol

EncodedData = bytes
T = TypeVar("T", bound="DataClassMessagePackMixin")
DEFAULT_DICT_PARAMS: Any

class Encoder:
    def __call__(self, o: Any, **kwargs: Any) -> EncodedData: ...

class Decoder:
    def __call__(self, packed: EncodedData, **kwargs: Any) -> Dict[Any, Any]: ...

def default_encoder(data: Any) -> EncodedData: ...
def default_decoder(data: EncodedData) -> Dict[Any, Any]: ...

class DataClassMessagePackMixin(DataClassDictMixin):
    def to_msgpack(self, encoder: Encoder = ..., **to_dict_kwargs: Any) -> EncodedData: ...
    @classmethod
    def from_msgpack(
        cls: Type[T], data: EncodedData, decoder: Decoder = ..., **from_dict_kwargs: Any
    ) -> T: ...
