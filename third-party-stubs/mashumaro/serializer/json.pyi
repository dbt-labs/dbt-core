from mashumaro.serializer.base import DataClassDictMixin
from typing import Any, Dict, Mapping, TypeVar, Union
from typing_extensions import Protocol as Protocol

DEFAULT_DICT_PARAMS: Any
EncodedData = Union[str, bytes, bytearray]
T = TypeVar("T", bound="DataClassJSONMixin")

class Encoder:
    def __call__(self, obj, **kwargs) -> EncodedData: ...

class Decoder:
    def __call__(self, s: EncodedData, **kwargs) -> Dict[Any, Any]: ...

class DataClassJSONMixin(DataClassDictMixin):
    def to_json(
        self, encoder: Encoder = ..., dict_params: Mapping = ..., **encoder_kwargs
    ) -> EncodedData: ...
    @classmethod
    def from_json(
        cls,
        data: EncodedData,
        decoder: Decoder = ...,
        dict_params: Mapping = ...,
        **decoder_kwargs,
    ) -> T: ...
