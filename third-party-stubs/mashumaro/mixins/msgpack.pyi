from mashumaro.mixins.dict import DataClassDictMixin
from typing import Any, Dict, TypeVar
from typing_extensions import Protocol as Protocol

EncodedData = bytes
T = TypeVar("T", bound="DataClassMessagePackMixin")
DEFAULT_DICT_PARAMS: Any

class Encoder:
    def __call__(self, o, **kwargs) -> EncodedData: ...

class Decoder:
    def __call__(self, packed: EncodedData, **kwargs) -> Dict[Any, Any]: ...

def default_encoder(data) -> EncodedData: ...
def default_decoder(data: EncodedData) -> Dict[Any, Any]: ...

class DataClassMessagePackMixin(DataClassDictMixin):
    def to_msgpack(self, encoder: Encoder = ..., **to_dict_kwargs) -> EncodedData: ...
    @classmethod
    def from_msgpack(cls, data: EncodedData, decoder: Decoder = ..., **from_dict_kwargs) -> T: ...
