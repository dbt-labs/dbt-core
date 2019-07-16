from hologram import FieldEncoder, JsonSchemaMixin
from typing import Type, NewType


def NewRangedInteger(name: str, minimum: int, maximum: int) -> Type:
    ranged = NewType(name, int)

    class RangeEncoder(FieldEncoder):
        @property
        def json_schema(self):
            return {'type': 'integer', 'minimum': minimum, 'maximum': maximum}

    JsonSchemaMixin.register_field_encoders({ranged: RangeEncoder()})
    return ranged


Port = NewRangedInteger('Port', minimum=0, maximum=65535)
