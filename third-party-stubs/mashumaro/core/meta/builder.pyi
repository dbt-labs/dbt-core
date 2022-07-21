from mashumaro.core.helpers import *
import types
import typing
from base64 import decodebytes as decodebytes, encodebytes as encodebytes
from dataclasses import Field
from mashumaro.config import (
    ADD_DIALECT_SUPPORT as ADD_DIALECT_SUPPORT,
    BaseConfig as BaseConfig,
    TO_DICT_ADD_BY_ALIAS_FLAG as TO_DICT_ADD_BY_ALIAS_FLAG,
    TO_DICT_ADD_OMIT_NONE_FLAG as TO_DICT_ADD_OMIT_NONE_FLAG,
)
from mashumaro.core.const import PY_39_MIN as PY_39_MIN
from mashumaro.core.meta.helpers import (
    get_args as get_args,
    get_class_that_defines_field as get_class_that_defines_field,
    get_class_that_defines_method as get_class_that_defines_method,
    get_literal_values as get_literal_values,
    get_name_error_name as get_name_error_name,
    get_type_origin as get_type_origin,
    is_class_var as is_class_var,
    is_dataclass_dict_mixin as is_dataclass_dict_mixin,
    is_dataclass_dict_mixin_subclass as is_dataclass_dict_mixin_subclass,
    is_dialect_subclass as is_dialect_subclass,
    is_generic as is_generic,
    is_init_var as is_init_var,
    is_literal as is_literal,
    is_named_tuple as is_named_tuple,
    is_new_type as is_new_type,
    is_optional as is_optional,
    is_special_typing_primitive as is_special_typing_primitive,
    is_type_var as is_type_var,
    is_type_var_any as is_type_var_any,
    is_typed_dict as is_typed_dict,
    is_union as is_union,
    not_none_type_arg as not_none_type_arg,
    resolve_type_vars as resolve_type_vars,
    type_name as type_name,
)
from mashumaro.core.meta.patch import patch_fromisoformat as patch_fromisoformat
from mashumaro.dialect import Dialect as Dialect
from mashumaro.exceptions import (
    BadDialect as BadDialect,
    BadHookSignature as BadHookSignature,
    InvalidFieldValue as InvalidFieldValue,
    MissingField as MissingField,
    ThirdPartyModuleNotFoundError as ThirdPartyModuleNotFoundError,
    UnresolvedTypeReferenceError as UnresolvedTypeReferenceError,
    UnserializableDataError as UnserializableDataError,
    UnserializableField as UnserializableField,
    UnsupportedDeserializationEngine as UnsupportedDeserializationEngine,
    UnsupportedSerializationEngine as UnsupportedSerializationEngine,
)
from mashumaro.helper import pass_through as pass_through
from mashumaro.types import (
    GenericSerializableType as GenericSerializableType,
    SerializableType as SerializableType,
    SerializationStrategy as SerializationStrategy,
)
from typing import Any

NoneType: Any
__PRE_SERIALIZE__: str
__PRE_DESERIALIZE__: str
__POST_SERIALIZE__: str
__POST_DESERIALIZE__: str

class CodeLines:
    def __init__(self) -> None: ...
    def append(self, line: str) -> None: ...
    def indent(self) -> typing.Generator[None, None, None]: ...
    def as_text(self) -> str: ...
    def reset(self) -> None: ...

class CodeBuilder:
    cls: Any = ...
    lines: Any = ...
    globals: Any = ...
    type_vars: Any = ...
    field_classes: Any = ...
    initial_arg_types: Any = ...
    dialect: Any = ...
    allow_postponed_evaluation: Any = ...
    def __init__(
        self,
        cls: Any,
        arg_types: typing.Tuple = ...,
        dialect: typing.Optional[typing.Type[Dialect]] = ...,
        first_method: str = ...,
        allow_postponed_evaluation: bool = ...,
    ) -> None: ...
    def reset(self) -> None: ...
    @property
    def namespace(self) -> typing.Dict[typing.Any, typing.Any]: ...
    @property
    def annotations(self) -> typing.Dict[str, typing.Any]: ...
    @property
    def field_types(self) -> typing.Dict[str, typing.Any]: ...
    @property
    def dataclass_fields(self) -> typing.Dict[str, Field]: ...
    @property
    def metadatas(self) -> typing.Dict[str, typing.Mapping[str, typing.Any]]: ...
    def get_field_default(self, name: str) -> typing.Any: ...
    def ensure_module_imported(self, module: types.ModuleType) -> None: ...
    def add_line(self, line: str) -> None: ...
    def indent(self) -> typing.Generator[None, None, None]: ...
    def compile(self) -> None: ...
    def get_declared_hook(self, method_name: str) -> typing.Any: ...
    def add_from_dict(self) -> None: ...
    def get_config(self, cls: Any = ...) -> typing.Type[BaseConfig]: ...
    def get_to_dict_flags(self, cls: Any = ...) -> str: ...
    def get_from_dict_flags(self, cls: Any = ...) -> str: ...
    def get_to_dict_default_flag_values(self, cls: Any = ...) -> str: ...
    def get_from_dict_default_flag_values(self, cls: Any = ...) -> str: ...
    def is_code_generation_option_enabled(self, option: str, cls: Any = ...) -> bool: ...
    def add_to_dict(self) -> None: ...
