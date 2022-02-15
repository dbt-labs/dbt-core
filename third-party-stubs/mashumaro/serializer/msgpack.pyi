from mashumaro.serializer.base import DataClassDictMixin as DataClassDictMixin
from typing import Any, Callable, Dict, Mapping, Type, TypeVar, Union

DEFAULT_DICT_PARAMS: Any
EncodedData = Union[str, bytes, bytearray]
Encoder = Callable[[Dict], EncodedData]
Decoder = Callable[[EncodedData], Dict]
T = TypeVar("T", bound="DataClassMessagePackMixin")

class DataClassMessagePackMixin(DataClassDictMixin):
    def to_msgpack(
        self, encoder: Encoder = ..., dict_params: Mapping = ..., **encoder_kwargs: Any
    ) -> EncodedData: ...
    @classmethod
    def from_msgpack(
        cls: Type[T],
        data: EncodedData,
        decoder: Decoder = ...,
        dict_params: Mapping = ...,
        **decoder_kwargs: Any,
    ) -> DataClassDictMixin: ...
