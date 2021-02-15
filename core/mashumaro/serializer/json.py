import json
from types import MappingProxyType
from typing import Any, Dict, Mapping, Type, TypeVar, Union

from typing_extensions import Protocol

from mashumaro.serializer.base import DataClassDictMixin

DEFAULT_DICT_PARAMS = {
    "use_bytes": False,
    "use_enum": False,
    "use_datetime": False,
}
EncodedData = Union[str, bytes, bytearray]
T = TypeVar("T", bound="DataClassJSONMixin")


class Encoder(Protocol):  # pragma no cover
    def __call__(self, obj, **kwargs) -> EncodedData:
        ...


class Decoder(Protocol):  # pragma no cover
    def __call__(self, s: EncodedData, **kwargs) -> Dict[Any, Any]:
        ...


class DataClassJSONMixin(DataClassDictMixin):
    def to_json(
        self: T,
        encoder: Encoder = json.dumps,
        dict_params: Mapping = MappingProxyType({}),
        **encoder_kwargs,
    ) -> EncodedData:

        return encoder(
            self.to_dict(**dict(DEFAULT_DICT_PARAMS, **dict_params)),
            **encoder_kwargs,
        )

    @classmethod
    def from_json(
        cls: Type[T],
        data: EncodedData,
        decoder: Decoder = json.loads,
        dict_params: Mapping = MappingProxyType({}),
        **decoder_kwargs,
    ) -> T:

        return cls.from_dict(
            decoder(data, **decoder_kwargs),
            **dict(DEFAULT_DICT_PARAMS, **dict_params),
        )
