from _typeshed import Incomplete
from enum import Enum
from mashumaro.config import BaseConfig as BaseConfig
from mashumaro.helper import pass_through as pass_through
from mashumaro.jsonschema.dialects import (
    DRAFT_2020_12 as DRAFT_2020_12,
    JSONSchemaDialect as JSONSchemaDialect,
)
from mashumaro.mixins.json import DataClassJSONMixin as DataClassJSONMixin
from typing import Any, Dict, List, Optional, Set, Union
from typing_extensions import TypeAlias

Number: TypeAlias = Union[int, float]
Null = object()

class JSONSchemaInstanceType(Enum):
    NULL: str
    BOOLEAN: str
    OBJECT: str
    ARRAY: str
    NUMBER: str
    STRING: str
    INTEGER: str

class JSONSchemaInstanceFormat(Enum): ...

class JSONSchemaStringFormat(JSONSchemaInstanceFormat):
    DATETIME: str
    DATE: str
    TIME: str
    DURATION: str
    EMAIL: str
    IDN_EMAIL: str
    HOSTNAME: str
    IDN_HOSTNAME: str
    IPV4ADDRESS: str
    IPV6ADDRESS: str
    URI: str
    URI_REFERENCE: str
    IRI: str
    IRI_REFERENCE: str
    UUID: str
    URI_TEMPLATE: str
    JSON_POINTER: str
    RELATIVE_JSON_POINTER: str
    REGEX: str

class JSONSchemaInstanceFormatExtension(JSONSchemaInstanceFormat):
    TIMEDELTA: str
    TIME_ZONE: str
    IPV4NETWORK: str
    IPV6NETWORK: str
    IPV4INTERFACE: str
    IPV6INTERFACE: str
    DECIMAL: str
    FRACTION: str
    BASE64: str
    PATH: str

DATETIME_FORMATS: Incomplete
IPADDRESS_FORMATS: Incomplete

class JSONSchema(DataClassJSONMixin):
    schema: Optional[str]
    type: Optional[JSONSchemaInstanceType]
    enum: Optional[List[Any]]
    const: Optional[Any]
    format: Optional[
        Union[JSONSchemaInstanceFormat, JSONSchemaStringFormat, JSONSchemaInstanceFormatExtension]
    ]
    title: Optional[str]
    description: Optional[str]
    anyOf: Optional[List["JSONSchema"]]
    reference: Optional[str]
    definitions: Optional[Dict[str, "JSONSchema"]]
    default: Optional[Any]
    deprecated: Optional[bool]
    examples: Optional[List[Any]]
    properties: Optional[Dict[str, "JSONSchema"]]
    patternProperties: Optional[Dict[str, "JSONSchema"]]
    additionalProperties: Union["JSONSchema", bool, None]
    propertyNames: Optional["JSONSchema"]
    prefixItems: Optional[List["JSONSchema"]]
    items: Optional["JSONSchema"]
    contains: Optional["JSONSchema"]
    multipleOf: Optional[Number]
    maximum: Optional[Number]
    exclusiveMaximum: Optional[Number]
    minimum: Optional[Number]
    exclusiveMinimum: Optional[Number]
    maxLength: Optional[int]
    minLength: Optional[int]
    pattern: Optional[str]
    maxItems: Optional[int]
    minItems: Optional[int]
    uniqueItems: Optional[bool]
    maxContains: Optional[int]
    minContains: Optional[int]
    maxProperties: Optional[int]
    minProperties: Optional[int]
    required: Optional[List[str]]
    dependentRequired: Optional[Dict[str, Set[str]]]

    class Config(BaseConfig):
        omit_none: bool
        serialize_by_alias: bool
        aliases: Incomplete
        serialization_strategy: Incomplete
    def __pre_serialize__(self) -> JSONSchema: ...
    def __post_serialize__(self, d: Dict[Any, Any]) -> Dict[Any, Any]: ...
    def __init__(
        self,
        schema,
        type,
        enum,
        const,
        format,
        title,
        description,
        anyOf,
        reference,
        definitions,
        default,
        deprecated,
        examples,
        properties,
        patternProperties,
        additionalProperties,
        propertyNames,
        prefixItems,
        items,
        contains,
        multipleOf,
        maximum,
        exclusiveMaximum,
        minimum,
        exclusiveMinimum,
        maxLength,
        minLength,
        pattern,
        maxItems,
        minItems,
        uniqueItems,
        maxContains,
        minContains,
        maxProperties,
        minProperties,
        required,
        dependentRequired,
    ) -> None: ...

class JSONObjectSchema(JSONSchema):
    type: JSONSchemaInstanceType
    def __init__(
        self,
        schema,
        type,
        enum,
        const,
        format,
        title,
        description,
        anyOf,
        reference,
        definitions,
        default,
        deprecated,
        examples,
        properties,
        patternProperties,
        additionalProperties,
        propertyNames,
        prefixItems,
        items,
        contains,
        multipleOf,
        maximum,
        exclusiveMaximum,
        minimum,
        exclusiveMinimum,
        maxLength,
        minLength,
        pattern,
        maxItems,
        minItems,
        uniqueItems,
        maxContains,
        minContains,
        maxProperties,
        minProperties,
        required,
        dependentRequired,
    ) -> None: ...

class JSONArraySchema(JSONSchema):
    type: JSONSchemaInstanceType
    def __init__(
        self,
        schema,
        type,
        enum,
        const,
        format,
        title,
        description,
        anyOf,
        reference,
        definitions,
        default,
        deprecated,
        examples,
        properties,
        patternProperties,
        additionalProperties,
        propertyNames,
        prefixItems,
        items,
        contains,
        multipleOf,
        maximum,
        exclusiveMaximum,
        minimum,
        exclusiveMinimum,
        maxLength,
        minLength,
        pattern,
        maxItems,
        minItems,
        uniqueItems,
        maxContains,
        minContains,
        maxProperties,
        minProperties,
        required,
        dependentRequired,
    ) -> None: ...

class Context:
    dialect: JSONSchemaDialect
    definitions: Dict[str, JSONSchema]
    all_refs: Optional[bool]
    ref_prefix: Optional[str]
    def __init__(self, dialect, definitions, all_refs, ref_prefix) -> None: ...
