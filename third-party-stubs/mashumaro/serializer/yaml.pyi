from mashumaro.serializer.base import DataClassDictMixin
from typing import Any, Dict, Mapping, TypeVar, Union
from typing_extensions import Protocol as Protocol

DEFAULT_DICT_PARAMS: Any
EncodedData = Union[str, bytes]
T = TypeVar("T", bound="DataClassYAMLMixin")

class Encoder:
    def __call__(self, o, **kwargs) -> EncodedData: ...

class Decoder:
    def __call__(self, packed: EncodedData, **kwargs) -> Dict[Any, Any]: ...

class DataClassYAMLMixin(DataClassDictMixin):
    def to_yaml(
        self, encoder: Encoder = ..., dict_params: Mapping = ..., **encoder_kwargs
    ) -> EncodedData: ...
    @classmethod
    def from_yaml(
        cls,
        data: EncodedData,
        decoder: Decoder = ...,
        dict_params: Mapping = ...,
        **decoder_kwargs,
    ) -> T: ...
