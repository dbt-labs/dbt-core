from mashumaro.mixins.dict import DataClassDictMixin
from typing import Any, Dict, TypeVar, Union
from typing_extensions import Protocol as Protocol

EncodedData = Union[str, bytes, bytearray]
T = TypeVar("T", bound="DataClassJSONMixin")

class Encoder:
    def __call__(self, obj, **kwargs) -> EncodedData: ...

class Decoder:
    def __call__(self, s: EncodedData, **kwargs) -> Dict[Any, Any]: ...

class DataClassJSONMixin(DataClassDictMixin):
    def to_json(self, encoder: Encoder = ..., **to_dict_kwargs) -> EncodedData: ...
    @classmethod
    def from_json(cls, data: EncodedData, decoder: Decoder = ..., **from_dict_kwargs) -> T: ...
