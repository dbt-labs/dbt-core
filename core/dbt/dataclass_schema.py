from typing import (
    Type, ClassVar, Dict, cast, TypeVar
)
import re
from dataclasses import fields
from enum import Enum
from datetime import datetime
from dateutil.parser import parse

from hologram import JsonSchemaMixin, FieldEncoder, ValidationError

from mashumaro import DataClassDictMixin
from mashumaro.types import SerializableEncoder, SerializableType


class DateTimeSerializableEncoder(SerializableEncoder[datetime]):
    @classmethod
    def _serialize(cls, value: datetime) -> str:
        out = value.isoformat()
        # Assume UTC if timezone is missing
        if value.tzinfo is None:
            out = out + "Z"
        return out

    @classmethod
    def _deserialize(cls, value: str) -> datetime:
        return (
            value if isinstance(value, datetime) else parse(cast(str, value))
        )


TV = TypeVar("TV")


# This class pulls in both JsonSchemaMixin from Hologram and
# DataClassDictMixin from our fork of Mashumaro. The 'to_dict'
# and 'from_dict' methods come from Mashumaro. Building
# jsonschemas for every class and the 'validate' method
# come from Hologram.
class dbtClassMixin(DataClassDictMixin, JsonSchemaMixin):
    """Mixin which adds methods to generate a JSON schema and
       convert to and from JSON encodable dicts with validation
       against the schema
    """

    _serializable_encoders: ClassVar[Dict[str, SerializableEncoder]] = {
        'datetime.datetime': DateTimeSerializableEncoder(),
    }
    _hyphenated: ClassVar[bool] = False
    ADDITIONAL_PROPERTIES: ClassVar[bool] = False

    # This is called by the mashumaro to_dict in order to handle
    # nested classes.
    # Munges the dict that's returned.
    def __post_serialize__(self, dct, options=None):
        keep_none = False
        if options and 'keep_none' in options and options['keep_none']:
            keep_none = True
        if not keep_none:  # remove attributes that are None
            new_dict = {k: v for k, v in dct.items() if v is not None}
            dct = new_dict

        if self._hyphenated:
            new_dict = {}
            for key in dct:
                if '_' in key:
                    new_key = key.replace('_', '-')
                    new_dict[new_key] = dct[key]
                else:
                    new_dict[key] = dct[key]
            dct = new_dict

        return dct

    # This is called by the mashumaro _from_dict method, before
    # performing the conversion to a dict
    @classmethod
    def __pre_deserialize__(cls, data, options=None):
        if cls._hyphenated:
            new_dict = {}
            for key in data:
                if '-' in key:
                    new_key = key.replace('-', '_')
                    new_dict[new_key] = data[key]
                else:
                    new_dict[key] = data[key]
            data = new_dict
        return data

    # This is used in the hologram._encode_field method, which calls
    # a 'to_dict' method which does not have the same parameters in
    # hologram and in mashumaro.
    def _local_to_dict(self, **kwargs):
        args = {}
        if 'omit_none' in kwargs and kwargs['omit_none'] is False:
            args['options'] = {'keep_none': True}
        return self.to_dict(**args)


class ValidatedStringMixin(str, SerializableType):
    ValidationRegex = ''

    @classmethod
    def _deserialize(cls, value: str) -> 'ValidatedStringMixin':
        cls.validate(value)
        return ValidatedStringMixin(value)

    def _serialize(self) -> str:
        return str(self)

    @classmethod
    def validate(cls, value):
        res = re.match(cls.ValidationRegex, value)

        if res is None:
            raise ValidationError(f"Invalid value: {value}")  # TODO


# These classes must be in this order or it doesn't work
class StrEnum(str, SerializableType, Enum):
    def __str__(self):
        return self.value

    # https://docs.python.org/3.6/library/enum.html#using-automatic-values
    def _generate_next_value_(name, *_):
        return name

    def _serialize(self) -> str:
        return self.value

    @classmethod
    def _deserialize(cls, value: str):
        return cls(value)


class HyphenatedDbtClassMixin(dbtClassMixin):
    # used by from_dict/to_dict
    _hyphenated: ClassVar[bool] = True

    # used by jsonschema validation, _get_fields
    @classmethod
    def field_mapping(cls):
        result = {}
        for field in fields(cls):
            skip = field.metadata.get("preserve_underscore")
            if skip:
                continue

            if "_" in field.name:
                result[field.name] = field.name.replace("_", "-")
        return result


class ExtensibleDbtClassMixin(dbtClassMixin):
    ADDITIONAL_PROPERTIES = True


# This is used by Hologram in jsonschema validation
def register_pattern(base_type: Type, pattern: str) -> None:
    """base_type should be a typing.NewType that should always have the given
    regex pattern. That means that its underlying type ('__supertype__') had
    better be a str!
    """

    class PatternEncoder(FieldEncoder):
        @property
        def json_schema(self):
            return {"type": "string", "pattern": pattern}

    dbtClassMixin.register_field_encoders({base_type: PatternEncoder()})
