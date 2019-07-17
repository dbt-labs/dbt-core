from hologram import FieldEncoder, JsonSchemaMixin
from typing import NewType


Port = NewType('Port', int)


class PortEncoder(FieldEncoder):
    @property
    def json_schema(self):
        return {'type': 'integer', 'minimum': 0, 'maximum': 65535}


JsonSchemaMixin.register_field_encoders({Port: PortEncoder()})
