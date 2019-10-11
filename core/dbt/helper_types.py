# never name this package "types", or mypy will crash in ugly ways
from datetime import timedelta
from numbers import Real
from typing import NewType

from hologram import (
    FieldEncoder, JsonSchemaMixin, JsonDict, ValidationError
)


Port = NewType('Port', int)


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


class RealEncoder(FieldEncoder):
    @property
    def json_schema(self):
        return {'type': 'number'}


JsonSchemaMixin.register_field_encoders({
    Port: PortEncoder(),
    timedelta: TimeDeltaFieldEncoder(),
    Real: RealEncoder(),
})
