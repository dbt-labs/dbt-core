from _typeshed import Incomplete
from functools import cached_property
from mashumaro.core.const import PEP_585_COMPATIBLE as PEP_585_COMPATIBLE
from mashumaro.core.meta.code.builder import CodeBuilder as CodeBuilder
from mashumaro.core.meta.helpers import (
    get_type_origin as get_type_origin,
    is_annotated as is_annotated,
    is_generic as is_generic,
    type_name as type_name,
)
from mashumaro.exceptions import (
    UnserializableDataError as UnserializableDataError,
    UnserializableField as UnserializableField,
)
from typing import Any, Dict, Mapping, Optional, Sequence, Type, TypeVar
from typing_extensions import TypeAlias

cached_property = property
NoneType: Incomplete
Expression: TypeAlias
P: Incomplete
T = TypeVar("T")

class ExpressionWrapper:
    expression: Incomplete
    def __init__(self, expression: str) -> None: ...

PROPER_COLLECTION_TYPES: Dict[Type, str]

class FieldContext:
    name: str
    metadata: Mapping
    def copy(self, **changes: Any) -> FieldContext: ...
    def __init__(self, name, metadata) -> None: ...

class ValueSpec:
    type: Type
    origin_type: Type
    expression: Expression
    builder: CodeBuilder
    field_ctx: FieldContext
    could_be_none: bool
    annotated_type: Optional[Type]
    def __setattr__(self, key: str, value: Any) -> None: ...
    def copy(self, **changes: Any) -> ValueSpec: ...
    @cached_property
    def annotations(self) -> Sequence[str]: ...
    def __init__(
        self, type, expression, builder, field_ctx, could_be_none, annotated_type
    ) -> None: ...

ValueSpecExprCreator: TypeAlias

class Registry:
    def register(self, function: ValueSpecExprCreator) -> ValueSpecExprCreator: ...
    def get(self, spec: ValueSpec) -> Expression: ...
    def __init__(self, _registry) -> None: ...

def ensure_generic_collection(spec: ValueSpec) -> bool: ...
def ensure_collection_type_args_supported(
    collection_type: Type, type_args: Sequence[Type]
) -> bool: ...
def ensure_generic_collection_subclass(spec: ValueSpec, *checked_types: Type) -> bool: ...
def ensure_generic_mapping(spec: ValueSpec, args: Sequence[Type], checked_type: Type) -> bool: ...
def expr_or_maybe_none(spec: ValueSpec, new_expr: Expression) -> Expression: ...
def random_hex() -> str: ...
def clean_id(value: str) -> str: ...
