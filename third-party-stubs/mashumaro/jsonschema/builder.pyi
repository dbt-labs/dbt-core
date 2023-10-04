from _typeshed import Incomplete
from mashumaro.jsonschema.dialects import JSONSchemaDialect
from mashumaro.jsonschema.models import Context, JSONSchema
from mashumaro.mixins.json import DataClassJSONMixin
from typing import Any, Dict, List, Optional, Type

def build_json_schema(
    instance_type: Type,
    context: Optional[Context] = ...,
    with_definitions: bool = ...,
    all_refs: Optional[bool] = ...,
    with_dialect_uri: bool = ...,
    dialect: Optional[JSONSchemaDialect] = ...,
    ref_prefix: Optional[str] = ...,
) -> JSONSchema: ...

class JSONSchemaDefinitions(DataClassJSONMixin):
    definitions: Dict[str, JSONSchema]
    def __post_serialize__(self, d: Dict[Any, Any]) -> List[Dict[str, Any]]: ...  # type: ignore
    def __init__(self, definitions) -> None: ...

class JSONSchemaBuilder:
    context: Incomplete
    def __init__(
        self,
        dialect: JSONSchemaDialect = ...,
        all_refs: Optional[bool] = ...,
        ref_prefix: Optional[str] = ...,
    ) -> None: ...
    def build(self, instance_type: Type) -> JSONSchema: ...
    def get_definitions(self) -> JSONSchemaDefinitions: ...
