from _typeshed import Incomplete
from mashumaro.mixins.dict import DataClassDictMixin as DataClassDictMixin
from typing import Any, Callable, Dict, TypeVar, Union

T = TypeVar("T", bound="DataClassYAMLMixin")
EncodedData = Union[str, bytes]
Encoder = Callable[[Any], EncodedData]
Decoder = Callable[[EncodedData], Dict[Any, Any]]
DefaultLoader: Incomplete
DefaultDumper: Incomplete

def default_encoder(data: Any) -> EncodedData: ...
def default_decoder(data: EncodedData) -> Dict[Any, Any]: ...

class DataClassYAMLMixin(DataClassDictMixin):
    def to_yaml(self, encoder: Encoder = ..., **to_dict_kwargs: Any) -> EncodedData: ...
    @classmethod
    def from_yaml(
        cls, data: EncodedData, decoder: Decoder = ..., **from_dict_kwargs: Any
    ) -> T: ...
