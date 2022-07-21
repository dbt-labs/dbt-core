from mashumaro.mixins.dict import DataClassDictMixin as DataClassDictMixin
from typing import Any, Dict, Type, TypeVar, Union
from typing_extensions import Protocol as Protocol

EncodedData = Union[str, bytes]
T = TypeVar("T", bound="DataClassYAMLMixin")

class Encoder:
    def __call__(self, o: Any, **kwargs: Any) -> EncodedData: ...

class Decoder:
    def __call__(self, packed: EncodedData, **kwargs: Any) -> Dict[Any, Any]: ...

DefaultLoader: Any
DefaultDumper: Any

def default_encoder(data: Any) -> EncodedData: ...
def default_decoder(data: EncodedData) -> Dict[Any, Any]: ...

class DataClassYAMLMixin(DataClassDictMixin):
    def to_yaml(self, encoder: Encoder = ..., **to_dict_kwargs: Any) -> EncodedData: ...
    @classmethod
    def from_yaml(
        cls: Type[T], data: EncodedData, decoder: Decoder = ..., **from_dict_kwargs: Any
    ) -> T: ...
