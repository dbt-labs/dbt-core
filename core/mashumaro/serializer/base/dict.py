# this file has been modified for use with dbt
from typing import Any, Dict, Mapping, Type, TypeVar, Optional, ClassVar

from mashumaro.serializer.base.metaprogramming import CodeBuilder
from mashumaro.types import SerializableEncoder

T = TypeVar("T", bound="DataClassDictMixin")


class DataClassDictMixin:
    _serializable_encoders: ClassVar[Dict[Type, SerializableEncoder]] = {}

    def __init_subclass__(cls: Type[T], **kwargs):
        builder = CodeBuilder(cls)
        exc = None
        try:
            builder.add_from_dict()
        except Exception as e:
            exc = e
        try:
            builder.add_to_dict()
        except Exception as e:
            exc = e
        if exc:
            raise exc

    def to_dict(
        self: T,
        use_bytes: bool = False,
        use_enum: bool = False,
        use_datetime: bool = False,
        options: Optional[Dict[str, Any]] = None,
    ) -> dict:
        ...

    @classmethod
    def from_dict(
        cls: Type[T],
        d: Mapping,
        use_bytes: bool = False,
        use_enum: bool = False,
        use_datetime: bool = False,
        options: Optional[Dict[str, Any]] = None,
    ) -> T:
        ...

    @classmethod
    def __pre_deserialize__(cls: Type[T], d: Dict[Any, Any], options: Optional[Dict[str, Any]]) -> Dict[Any, Any]:
        ...

    @classmethod
    def __post_deserialize__(cls: Type[T], obj: T, options: Optional[Dict[str, Any]]) -> T:
        ...

    def __pre_serialize__(self: T, options: Optional[Dict[str, Any]]) -> T:
        ...

    def __post_serialize__(self: T, d: Dict[Any, Any], options: Optional[Dict[str, Any]]) -> Dict[Any, Any]:
        ...

__all__ = ["DataClassDictMixin"]
