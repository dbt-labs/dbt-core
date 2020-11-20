# never name this package "types", or mypy will crash in ugly ways
from dataclasses import dataclass
from datetime import timedelta
from pathlib import Path
from typing import Tuple, AbstractSet, Union

from dbt.dataclass_schema import (
    dbtClassMixin, ValidationError, StrEnum,
)
from hologram import FieldEncoder, JsonDict
from mashumaro.types import SerializableType


class Port(int, SerializableType):
    @classmethod
    def _deserialize(cls, value: Union[int, str]) -> 'Port':
        try:
            value = int(value)
        except ValueError:
            raise ValidationError(f'Cannot encode {value} into port number')

        return Port(value)

    def _serialize(self) -> int:
        return self


class PortEncoder(FieldEncoder):
    @property
    def json_schema(self):
        return {'type': 'integer', 'minimum': 0, 'maximum': 65535}


class TimeDeltaFieldEncoder(FieldEncoder[timedelta]):
    """Encodes timedeltas to dictionaries"""

    def to_wire(self, value: timedelta) -> float:
        return value.total_seconds()

    def to_python(self, value) -> timedelta:
        if isinstance(value, timedelta):
            return value
        try:
            return timedelta(seconds=value)
        except TypeError:
            raise ValidationError(
                'cannot encode {} into timedelta'.format(value)
            ) from None

    @property
    def json_schema(self) -> JsonDict:
        return {'type': 'number'}


class PathEncoder(FieldEncoder):
    def to_wire(self, value: Path) -> str:
        return str(value)

    def to_python(self, value) -> Path:
        if isinstance(value, Path):
            return value
        try:
            return Path(value)
        except TypeError:
            raise ValidationError(
                'cannot encode {} into timedelta'.format(value)
            ) from None

    @property
    def json_schema(self) -> JsonDict:
        return {'type': 'string'}


class NVEnum(StrEnum):
    novalue = 'novalue'

    def __eq__(self, other):
        return isinstance(other, NVEnum)


@dataclass
class NoValue(dbtClassMixin):
    """Sometimes, you want a way to say none that isn't None"""
    novalue: NVEnum = NVEnum.novalue


dbtClassMixin.register_field_encoders({
    Port: PortEncoder(),
    timedelta: TimeDeltaFieldEncoder(),
    Path: PathEncoder(),
})


FQNPath = Tuple[str, ...]
PathSet = AbstractSet[FQNPath]
