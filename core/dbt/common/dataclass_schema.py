from typing import ClassVar, cast, get_type_hints, List, Tuple, Dict, Any, Optional
import re
import jsonschema
from dataclasses import fields, Field
from enum import Enum
from datetime import datetime
from dateutil.parser import parse

# type: ignore
from mashumaro import DataClassDictMixin
from mashumaro.config import TO_DICT_ADD_OMIT_NONE_FLAG, BaseConfig as MashBaseConfig
from mashumaro.types import SerializableType, SerializationStrategy
from mashumaro.jsonschema import build_json_schema

import functools


class ValidationError(jsonschema.ValidationError):
    pass


class DateTimeSerialization(SerializationStrategy):
    def serialize(self, value) -> str:
        out = value.isoformat()
        # Assume UTC if timezone is missing
        if value.tzinfo is None:
            out += "Z"
        return out

    def deserialize(self, value) -> datetime:
        return value if isinstance(value, datetime) else parse(cast(str, value))


class dbtMashConfig(MashBaseConfig):
    code_generation_options = [
        TO_DICT_ADD_OMIT_NONE_FLAG,
    ]
    serialization_strategy = {
        datetime: DateTimeSerialization(),
    }
    json_schema = {
        "additionalProperties": False,
    }
    serialize_by_alias = True


# This class pulls in DataClassDictMixin from Mashumaro. The 'to_dict'
# and 'from_dict' methods come from Mashumaro.
class dbtClassMixin(DataClassDictMixin):
    """The Mixin adds methods to generate a JSON schema and
    convert to and from JSON encodable dicts with validation
    against the schema
    """

    _mapped_fields: ClassVar[Optional[Dict[Any, List[Tuple[Field, str]]]]] = None

    # Config class used by Mashumaro
    class Config(dbtMashConfig):
        pass

    ADDITIONAL_PROPERTIES: ClassVar[bool] = False

    # This is called by the mashumaro from_dict in order to handle
    # nested classes. We no longer do any munging here, but leaving here
    # so that subclasses can leave super() in place for possible future needs.
    @classmethod
    def __pre_deserialize__(cls, data):
        return data

    # This is called by the mashumaro to_dict in order to handle
    # nested classes. We no longer do any munging here, but leaving here
    # so that subclasses can leave super() in place for possible future needs.
    def __post_serialize__(self, data):
        return data

    @classmethod
    @functools.lru_cache
    def json_schema(cls):
        json_schema_obj = build_json_schema(cls)
        json_schema = json_schema_obj.to_dict()
        return json_schema

    @classmethod
    def validate(cls, data):
        json_schema = cls.json_schema()
        validator = jsonschema.Draft7Validator(json_schema)
        error = next(iter(validator.iter_errors(data)), None)
        if error is not None:
            raise ValidationError.create_from(error) from error

    # This method was copied from hologram. Used in model_config.py and relation.py
    @classmethod
    def _get_fields(cls) -> List[Tuple[Field, str]]:
        if cls._mapped_fields is None:
            cls._mapped_fields = {}
        if cls.__name__ not in cls._mapped_fields:
            mapped_fields = []
            type_hints = get_type_hints(cls)

            for f in fields(cls):  # type: ignore
                # Skip internal fields
                if f.name.startswith("_"):
                    continue

                # Note fields() doesn't resolve forward refs
                f.type = type_hints[f.name]

                # hologram used the "field_mapping" here, but we use the
                # the field's metadata "alias". Since this method is mainly
                # just used in merging config dicts, it mostly applies to
                # pre-hook and post-hook.
                field_name = f.metadata.get("alias", f.name)
                mapped_fields.append((f, field_name))
            cls._mapped_fields[cls.__name__] = mapped_fields
        return cls._mapped_fields[cls.__name__]

    # copied from hologram. Used in tests
    @classmethod
    def _get_field_names(cls):
        return [element[1] for element in cls._get_fields()]


class ValidatedStringMixin(str, SerializableType):
    ValidationRegex = ""

    @classmethod
    def _deserialize(cls, value: str) -> "ValidatedStringMixin":
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


class ExtensibleDbtClassMixin(dbtClassMixin):
    ADDITIONAL_PROPERTIES = True

    class Config(dbtMashConfig):
        json_schema = {
            "additionalProperties": True,
        }
