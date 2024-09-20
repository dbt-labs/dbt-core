from mashumaro.mixins.dict import DataClassDictMixin as DataClassDictMixin
from typing import Any, Callable, Dict, TypeVar, Union, Type

EncodedData = Union[str, bytes, bytearray]
T = TypeVar("T", bound="DataClassJSONMixin")

class Encoder:
    def __call__(self, obj: Any, **kwargs: Any) -> EncodedData: ...

class Decoder:
    def __call__(self, s: EncodedData, **kwargs: Any) -> Dict[Any, Any]: ...

class DataClassJSONMixin(DataClassDictMixin):
    def to_json(self, encoder: Encoder = ..., **to_dict_kwargs: Any) -> EncodedData: ...
    @classmethod
    def from_json(
        cls: Type[T], data: EncodedData, decoder: Decoder = ..., **from_dict_kwargs: Any
    ) -> T: ...
