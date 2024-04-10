import types
import typing
from _typeshed import Incomplete
from collections.abc import Generator
from dataclasses import Field
from mashumaro.config import (
    ADD_DIALECT_SUPPORT as ADD_DIALECT_SUPPORT,
    ADD_SERIALIZATION_CONTEXT as ADD_SERIALIZATION_CONTEXT,
    BaseConfig as BaseConfig,
    SerializationStrategyValueType as SerializationStrategyValueType,
    TO_DICT_ADD_BY_ALIAS_FLAG as TO_DICT_ADD_BY_ALIAS_FLAG,
    TO_DICT_ADD_OMIT_NONE_FLAG as TO_DICT_ADD_OMIT_NONE_FLAG,
)
from mashumaro.core.const import Sentinel as Sentinel
from mashumaro.core.helpers import ConfigValue as ConfigValue
from mashumaro.core.meta.code.lines import CodeLines as CodeLines
from mashumaro.core.meta.helpers import (
    get_args as get_args,
    get_class_that_defines_field as get_class_that_defines_field,
    get_class_that_defines_method as get_class_that_defines_method,
    get_literal_values as get_literal_values,
    get_name_error_name as get_name_error_name,
    hash_type_args as hash_type_args,
    is_class_var as is_class_var,
    is_dataclass_dict_mixin as is_dataclass_dict_mixin,
    is_dialect_subclass as is_dialect_subclass,
    is_init_var as is_init_var,
    is_literal as is_literal,
    is_optional as is_optional,
    is_type_var_any as is_type_var_any,
    resolve_type_params as resolve_type_params,
    substitute_type_params as substitute_type_params,
    type_name as type_name,
)
from mashumaro.core.meta.types.common import FieldContext as FieldContext, ValueSpec as ValueSpec
from mashumaro.core.meta.types.pack import PackerRegistry as PackerRegistry
from mashumaro.core.meta.types.unpack import (
    SubtypeUnpackerBuilder as SubtypeUnpackerBuilder,
    UnpackerRegistry as UnpackerRegistry,
)
from mashumaro.dialect import Dialect as Dialect
from mashumaro.exceptions import (
    BadDialect as BadDialect,
    BadHookSignature as BadHookSignature,
    InvalidFieldValue as InvalidFieldValue,
    MissingDiscriminatorError as MissingDiscriminatorError,
    MissingField as MissingField,
    SuitableVariantNotFoundError as SuitableVariantNotFoundError,
    ThirdPartyModuleNotFoundError as ThirdPartyModuleNotFoundError,
    UnresolvedTypeReferenceError as UnresolvedTypeReferenceError,
    UnserializableDataError as UnserializableDataError,
    UnserializableField as UnserializableField,
    UnsupportedDeserializationEngine as UnsupportedDeserializationEngine,
    UnsupportedSerializationEngine as UnsupportedSerializationEngine,
)
from mashumaro.types import Discriminator as Discriminator

__PRE_SERIALIZE__: str
__PRE_DESERIALIZE__: str
__POST_SERIALIZE__: str
__POST_DESERIALIZE__: str

class CodeBuilder:
    cls: Incomplete
    lines: Incomplete
    globals: Incomplete
    resolved_type_params: Incomplete
    field_classes: Incomplete
    initial_type_args: Incomplete
    dialect: Incomplete
    default_dialect: Incomplete
    allow_postponed_evaluation: Incomplete
    format_name: Incomplete
    decoder: Incomplete
    encoder: Incomplete
    encoder_kwargs: Incomplete
    def __init__(
        self,
        cls: typing.Type,
        type_args: typing.Tuple[typing.Type, ...] = ...,
        dialect: typing.Optional[typing.Type[Dialect]] = ...,
        first_method: str = ...,
        allow_postponed_evaluation: bool = ...,
        format_name: str = ...,
        decoder: typing.Optional[typing.Any] = ...,
        encoder: typing.Optional[typing.Any] = ...,
        encoder_kwargs: typing.Optional[typing.Dict[str, typing.Any]] = ...,
        default_dialect: typing.Optional[typing.Type[Dialect]] = ...,
    ) -> None: ...
    def reset(self) -> None: ...
    @property
    def namespace(self) -> typing.Mapping[typing.Any, typing.Any]: ...
    @property
    def annotations(self) -> typing.Dict[str, typing.Any]: ...
    def get_field_resolved_type_params(
        self, field_name: str
    ) -> typing.Dict[typing.Type, typing.Type]: ...
    def get_field_types(self, include_extras: bool = ...) -> typing.Dict[str, typing.Any]: ...
    @property
    def dataclass_fields(self) -> typing.Dict[str, Field]: ...
    @property
    def metadatas(self) -> typing.Dict[str, typing.Mapping[str, typing.Any]]: ...
    def get_field_default(self, name: str) -> typing.Any: ...
    def add_type_modules(self, *types_: typing.Type) -> None: ...
    def ensure_module_imported(self, module: types.ModuleType) -> None: ...
    def ensure_object_imported(
        self, obj: typing.Any, name: typing.Optional[str] = ...
    ) -> None: ...
    def add_line(self, line: str) -> None: ...
    def indent(self, expr: typing.Optional[str] = ...) -> typing.Generator[None, None, None]: ...
    def compile(self) -> None: ...
    def get_declared_hook(self, method_name: str) -> typing.Any: ...
    def add_unpack_method(self) -> None: ...
    def get_config(self, cls: Incomplete | None = ..., look_in_parents: bool = ...): ...
    def get_discriminator(self) -> typing.Optional[Discriminator]: ...
    def get_pack_method_flags(
        self, cls: typing.Optional[typing.Type] = ..., pass_encoder: bool = ...
    ) -> str: ...
    def get_unpack_method_flags(
        self, cls: typing.Optional[typing.Type] = ..., pass_decoder: bool = ...
    ) -> str: ...
    def get_pack_method_default_flag_values(
        self, cls: typing.Optional[typing.Type] = ..., pass_encoder: bool = ...
    ) -> str: ...
    def get_unpack_method_default_flag_values(self, pass_decoder: bool = ...) -> str: ...
    def is_code_generation_option_enabled(
        self, option: str, cls: typing.Optional[typing.Type] = ...
    ) -> bool: ...
    @classmethod
    def get_unpack_method_name(
        cls,
        type_args: typing.Iterable = ...,
        format_name: str = ...,
        decoder: typing.Optional[typing.Any] = ...,
    ) -> str: ...
    @classmethod
    def get_pack_method_name(
        cls,
        type_args: typing.Tuple[typing.Type, ...] = ...,
        format_name: str = ...,
        encoder: typing.Optional[typing.Any] = ...,
    ) -> str: ...
    def add_pack_method(self) -> None: ...
    def iter_serialization_strategies(
        self, metadata, ftype
    ) -> Generator[Incomplete, None, None]: ...
