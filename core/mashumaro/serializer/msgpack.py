from functools import partial
from types import MappingProxyType
from typing import Any, Dict, Mapping, Type, TypeVar, Union

import msgpack
from typing_extensions import Protocol

from mashumaro.serializer.base import DataClassDictMixin

DEFAULT_DICT_PARAMS = {
    "use_bytes": True,
    "use_enum": False,
    "use_datetime": False,
}
EncodedData = Union[str, bytes, bytearray]
T = TypeVar("T", bound="DataClassMessagePackMixin")


class Encoder(Protocol):  # pragma no cover
    def __call__(self, o, **kwargs) -> EncodedData:
        ...


class Decoder(Protocol):  # pragma no cover
    def __call__(self, packed: EncodedData, **kwargs) -> Dict[Any, Any]:
        ...


class DataClassMessagePackMixin(DataClassDictMixin):
    def to_msgpack(
        self: T,
        encoder: Encoder = partial(msgpack.packb, use_bin_type=True),
        dict_params: Mapping = MappingProxyType({}),
        **encoder_kwargs,
    ) -> EncodedData:

        return encoder(
            self.to_dict(**dict(DEFAULT_DICT_PARAMS, **dict_params)),
            **encoder_kwargs,
        )

    @classmethod
    def from_msgpack(
        cls: Type[T],
        data: EncodedData,
        decoder: Decoder = partial(msgpack.unpackb, raw=False),
        dict_params: Mapping = MappingProxyType({}),
        **decoder_kwargs,
    ) -> T:
        return cls.from_dict(
            decoder(data, **decoder_kwargs),
            **dict(DEFAULT_DICT_PARAMS, **dict_params),
        )
