from _typeshed import Incomplete
from mashumaro.core.meta.helpers import type_name as type_name
from typing import Any, Optional, Type

class MissingField(LookupError):
    field_name: Incomplete
    field_type: Incomplete
    holder_class: Incomplete
    def __init__(self, field_name: str, field_type: Type, holder_class: Type) -> None: ...
    @property
    def field_type_name(self) -> str: ...
    @property
    def holder_class_name(self) -> str: ...

class UnserializableDataError(TypeError): ...

class UnserializableField(UnserializableDataError):
    field_name: Incomplete
    field_type: Incomplete
    holder_class: Incomplete
    msg: Incomplete
    def __init__(
        self, field_name: str, field_type: Type, holder_class: Type, msg: Optional[str] = ...
    ) -> None: ...
    @property
    def field_type_name(self) -> str: ...
    @property
    def holder_class_name(self) -> str: ...

class UnsupportedSerializationEngine(UnserializableField):
    def __init__(
        self, field_name: str, field_type: Type, holder_class: Type, engine: Any
    ) -> None: ...

class UnsupportedDeserializationEngine(UnserializableField):
    def __init__(
        self, field_name: str, field_type: Type, holder_class: Type, engine: Any
    ) -> None: ...

class InvalidFieldValue(ValueError):
    field_name: Incomplete
    field_type: Incomplete
    field_value: Incomplete
    holder_class: Incomplete
    msg: Incomplete
    def __init__(
        self,
        field_name: str,
        field_type: Type,
        field_value: Any,
        holder_class: Type,
        msg: Optional[str] = ...,
    ) -> None: ...
    @property
    def field_type_name(self) -> str: ...
    @property
    def holder_class_name(self) -> str: ...

class MissingDiscriminatorError(LookupError):
    field_name: Incomplete
    def __init__(self, field_name: str) -> None: ...

class SuitableVariantNotFoundError(ValueError):
    variants_type: Incomplete
    discriminator_name: Incomplete
    discriminator_value: Incomplete
    def __init__(
        self,
        variants_type: Type,
        discriminator_name: Optional[str] = ...,
        discriminator_value: Any = ...,
    ) -> None: ...

class BadHookSignature(TypeError): ...

class ThirdPartyModuleNotFoundError(ModuleNotFoundError):
    module_name: Incomplete
    field_name: Incomplete
    holder_class: Incomplete
    def __init__(self, module_name: str, field_name: str, holder_class: Type) -> None: ...
    @property
    def holder_class_name(self) -> str: ...

class UnresolvedTypeReferenceError(NameError):
    holder_class: Incomplete
    name: Incomplete
    def __init__(self, holder_class: Type, unresolved_type_name: str) -> None: ...
    @property
    def holder_class_name(self) -> str: ...

class BadDialect(ValueError): ...
