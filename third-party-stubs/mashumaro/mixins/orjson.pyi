from mashumaro.dialect import Dialect as Dialect
from mashumaro.mixins.dict import DataClassDictMixin as DataClassDictMixin
from typing import Any, Callable, Dict, TypeVar, Union

T = TypeVar("T", bound="DataClassORJSONMixin")
EncodedData = Union[str, bytes, bytearray]
Encoder = Callable[[Any], EncodedData]
Decoder = Callable[[EncodedData], Dict[Any, Any]]

class OrjsonDialect(Dialect):
    serialization_strategy: Any

class DataClassORJSONMixin(DataClassDictMixin):
    def to_jsonb(
        self, encoder: Encoder = ..., *, orjson_options: int = ..., **to_dict_kwargs: Any
    ) -> bytes: ...
    def to_json(
        self, encoder: Encoder = ..., *, orjson_options: int = ..., **to_dict_kwargs: Any
    ) -> bytes: ...
    @classmethod
    def from_json(
        cls, data: EncodedData, decoder: Decoder = ..., **from_dict_kwargs: Any
    ) -> T: ...
