from _typeshed import Incomplete
from mashumaro.dialect import Dialect as Dialect
from mashumaro.helper import pass_through as pass_through
from mashumaro.mixins.dict import DataClassDictMixin as DataClassDictMixin
from typing import Any, Callable, Dict, TypeVar

T = TypeVar("T", bound="DataClassTOMLMixin")
EncodedData = str
Encoder = Callable[[Any], EncodedData]
Decoder = Callable[[EncodedData], Dict[Any, Any]]

class TOMLDialect(Dialect):
    omit_none: bool
    serialization_strategy: Incomplete

class DataClassTOMLMixin(DataClassDictMixin):
    def to_toml(self, encoder: Encoder = ..., **to_dict_kwargs: Any) -> EncodedData: ...
    @classmethod
    def from_toml(
        cls, data: EncodedData, decoder: Decoder = ..., **from_dict_kwargs: Any
    ) -> T: ...
