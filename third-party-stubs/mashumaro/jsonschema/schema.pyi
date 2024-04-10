from mashumaro.config import BaseConfig
from mashumaro.jsonschema.annotations import Annotation
from mashumaro.jsonschema.models import Context, JSONSchema
from typing import Any, Callable, Iterable, List, Mapping, Optional, Tuple, Type, Union

class Instance:
    type: Type
    name: Optional[str]
    origin_type: Type
    annotations: List[Annotation]
    @property
    def metadata(self) -> Mapping[str, Any]: ...
    @property
    def alias(self) -> Optional[str]: ...
    @property
    def holder_class(self) -> Optional[Type]: ...
    def copy(self, **changes: Any) -> Instance: ...
    def __post_init__(self) -> None: ...
    def update_type(self, new_type: Type) -> None: ...
    def fields(self) -> Iterable[Tuple[str, Type, bool, Any]]: ...
    def get_overridden_serialization_method(self) -> Optional[Union[Callable, str]]: ...
    def get_config(self) -> Type[BaseConfig]: ...
    def __init__(self, type, name, __builder) -> None: ...

class InstanceSchemaCreatorRegistry:
    def register(self, func: InstanceSchemaCreator) -> InstanceSchemaCreator: ...
    def iter(self) -> Iterable[InstanceSchemaCreator]: ...
    def __init__(self, _registry) -> None: ...

class EmptyJSONSchema(JSONSchema):
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

def get_schema(instance: Instance, ctx: Context, with_dialect_uri: bool = ...) -> JSONSchema: ...
