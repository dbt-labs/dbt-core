from mashumaro.mixins.dict import DataClassDictMixin
from typing import Any, Dict, TypeVar, Union
from typing_extensions import Protocol as Protocol

EncodedData = Union[str, bytes]
T = TypeVar("T", bound="DataClassYAMLMixin")

class Encoder:
    def __call__(self, o, **kwargs) -> EncodedData: ...

class Decoder:
    def __call__(self, packed: EncodedData, **kwargs) -> Dict[Any, Any]: ...

DefaultLoader: Any
DefaultDumper: Any

def default_encoder(data) -> EncodedData: ...
def default_decoder(data: EncodedData) -> Dict[Any, Any]: ...

class DataClassYAMLMixin(DataClassDictMixin):
    def to_yaml(self, encoder: Encoder = ..., **to_dict_kwargs) -> EncodedData: ...
    @classmethod
    def from_yaml(cls, data: EncodedData, decoder: Decoder = ..., **from_dict_kwargs) -> T: ...
