from mashumaro.serializer.base import DataClassDictMixin
from typing import Any, Dict, Mapping, TypeVar
from typing_extensions import Protocol as Protocol

DEFAULT_DICT_PARAMS: Any
EncodedData = bytes
T = TypeVar("T", bound="DataClassMessagePackMixin")

class Encoder:
    def __call__(self, o, **kwargs) -> EncodedData: ...

class Decoder:
    def __call__(self, packed: EncodedData, **kwargs) -> Dict[Any, Any]: ...

class DataClassMessagePackMixin(DataClassDictMixin):
    def to_msgpack(
        self, encoder: Encoder = ..., dict_params: Mapping = ..., **encoder_kwargs
    ) -> EncodedData: ...
    @classmethod
    def from_msgpack(
        cls,
        data: EncodedData,
        decoder: Decoder = ...,
        dict_params: Mapping = ...,
        **decoder_kwargs,
    ) -> T: ...
