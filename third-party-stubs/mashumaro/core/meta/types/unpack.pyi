import abc
from _typeshed import Incomplete
from abc import ABC, abstractmethod
from mashumaro.core.meta.types.common import ValueSpec
from mashumaro.types import Discriminator
from typing import Optional, Tuple, Type

UnpackerRegistry: Incomplete

class AbstractUnpackerBuilder(ABC, metaclass=abc.ABCMeta):
    @abstractmethod
    def get_method_prefix(self) -> str: ...
    def build(self, spec: ValueSpec) -> str: ...

class UnionUnpackerBuilder(AbstractUnpackerBuilder):
    union_args: Incomplete
    def __init__(self, args: Tuple[Type, ...]) -> None: ...
    def get_method_prefix(self) -> str: ...

class TypeVarUnpackerBuilder(UnionUnpackerBuilder):
    def get_method_prefix(self) -> str: ...

class LiteralUnpackerBuilder(AbstractUnpackerBuilder):
    def get_method_prefix(self) -> str: ...

class DiscriminatedUnionUnpackerBuilder(AbstractUnpackerBuilder):
    discriminator: Incomplete
    base_variants: Incomplete
    def __init__(
        self, discriminator: Discriminator, base_variants: Optional[Tuple[Type, ...]] = ...
    ) -> None: ...
    def get_method_prefix(self) -> str: ...

class SubtypeUnpackerBuilder(DiscriminatedUnionUnpackerBuilder): ...
