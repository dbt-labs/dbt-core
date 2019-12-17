# never name this package "types", or mypy will crash in ugly ways
from datetime import timedelta
from typing import NewType, Dict

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


class NoValue:
    """Sometimes, you want a way to say none that isn't None"""
    def __eq__(self, other):
        return isinstance(other, NoValue)


class NoValueEncoder(FieldEncoder):
    # the FieldEncoder class specifies a narrow range that only includes value
    # types (str, float, None) but we want to support something extra
    def to_wire(self, value: NoValue) -> Dict[str, str]:  # type: ignore
        return {'novalue': 'novalue'}

    def to_python(self, value) -> NoValue:
        if (
            not isinstance(value, dict) or
            'novalue' not in value or
            value['novalue'] != 'novalue'
        ):
            raise ValidationError('Got invalid NoValue: {}'.format(value))
        return NoValue()

    @property
    def json_schema(self):
        return {
            'type': 'object',
            'properties': {
                'novalue': {
                    'enum': ['novalue'],
                }
            }
        }


JsonSchemaMixin.register_field_encoders({
    Port: PortEncoder(),
    timedelta: TimeDeltaFieldEncoder(),
    NoValue: NoValueEncoder(),
})
