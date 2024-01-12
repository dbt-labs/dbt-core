from dataclasses import Field
from enum import Enum
from typing import TypeVar, Type, Optional, Dict, Any

from dbt.common.exceptions import DbtInternalError

M = TypeVar("M", bound="Metadata")


class Metadata(Enum):
    @classmethod
    def from_field(cls: Type[M], fld: Field) -> M:
        default = cls.default_field()
        key = cls.metadata_key()

        return _get_meta_value(cls, fld, key, default)

    def meta(self, existing: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        key = self.metadata_key()
        return _set_meta_value(self, key, existing)

    @classmethod
    def default_field(cls) -> "Metadata":
        raise NotImplementedError("Not implemented")

    @classmethod
    def metadata_key(cls) -> str:
        raise NotImplementedError("Not implemented")


def _get_meta_value(cls: Type[M], fld: Field, key: str, default: Any) -> M:
    # a metadata field might exist. If it does, it might have a matching key.
    # If it has both, make sure the value is valid and return it. If it
    # doesn't, return the default.
    if fld.metadata:
        value = fld.metadata.get(key, default)
    else:
        value = default

    try:
        return cls(value)
    except ValueError as exc:
        raise DbtInternalError(f"Invalid {cls} value: {value}") from exc


def _set_meta_value(obj: M, key: str, existing: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    if existing is None:
        result = {}
    else:
        result = existing.copy()
    result.update({key: obj})
    return result


class ShowBehavior(Metadata):
    Show = 1
    Hide = 2

    @classmethod
    def default_field(cls) -> "ShowBehavior":
        return cls.Show

    @classmethod
    def metadata_key(cls) -> str:
        return "show_hide"

    @classmethod
    def should_show(cls, fld: Field) -> bool:
        return cls.from_field(fld) == cls.Show
