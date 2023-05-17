from dbt.contracts.graph.unparsed import (
    HasColumnProps,
    UnparsedColumn,
    UnparsedNodeUpdate,
    UnparsedMacroUpdate,
    UnparsedAnalysisUpdate,
    UnparsedExposure,
    UnparsedModelUpdate,
)
from dbt.contracts.graph.unparsed import NodeVersion, HasColumnTests, HasColumnDocs
from dbt.contracts.graph.nodes import (
    UnpatchedSourceDefinition,
    ColumnInfo,
    ColumnLevelConstraint,
    ConstraintType,
)
from dbt.parser.search import FileBlock
from typing import List, Dict, Any, TypeVar, Generic, Union, Optional
from dataclasses import dataclass
from dbt.exceptions import DbtInternalError, ParsingError


def trimmed(inp: str) -> str:
    if len(inp) < 50:
        return inp
    return inp[:44] + "..." + inp[-3:]


TestDef = Union[str, Dict[str, Any]]


Target = TypeVar(
    "Target",
    UnparsedNodeUpdate,
    UnparsedMacroUpdate,
    UnparsedAnalysisUpdate,
    UnpatchedSourceDefinition,
    UnparsedExposure,
    UnparsedModelUpdate,
)


ColumnTarget = TypeVar(
    "ColumnTarget",
    UnparsedModelUpdate,
    UnparsedNodeUpdate,
    UnparsedAnalysisUpdate,
    UnpatchedSourceDefinition,
)

Versioned = TypeVar("Versioned", bound=UnparsedModelUpdate)

Testable = TypeVar("Testable", UnparsedNodeUpdate, UnpatchedSourceDefinition, UnparsedModelUpdate)


@dataclass
class YamlBlock(FileBlock):
    data: Dict[str, Any]

    @classmethod
    def from_file_block(cls, src: FileBlock, data: Dict[str, Any]):
        return cls(
            file=src.file,
            data=data,
        )


@dataclass
class TargetBlock(YamlBlock, Generic[Target]):
    target: Target

    @property
    def name(self):
        return self.target.name

    @property
    def columns(self):
        return []

    @property
    def tests(self) -> List[TestDef]:
        return []

    @classmethod
    def from_yaml_block(cls, src: YamlBlock, target: Target) -> "TargetBlock[Target]":
        return cls(
            file=src.file,
            data=src.data,
            target=target,
        )


@dataclass
class TargetColumnsBlock(TargetBlock[ColumnTarget], Generic[ColumnTarget]):
    @property
    def columns(self):
        if self.target.columns is None:
            return []
        else:
            return self.target.columns


@dataclass
class TestBlock(TargetColumnsBlock[Testable], Generic[Testable]):
    @property
    def tests(self) -> List[TestDef]:
        if self.target.tests is None:
            return []
        else:
            return self.target.tests

    @property
    def quote_columns(self) -> Optional[bool]:
        return self.target.quote_columns

    @classmethod
    def from_yaml_block(cls, src: YamlBlock, target: Testable) -> "TestBlock[Testable]":
        return cls(
            file=src.file,
            data=src.data,
            target=target,
        )


@dataclass
class VersionedTestBlock(TestBlock, Generic[Versioned]):
    @property
    def columns(self):
        if not self.target.versions:
            return super().columns
        else:
            raise DbtInternalError(".columns for VersionedTestBlock with versions")

    @property
    def tests(self) -> List[TestDef]:
        if not self.target.versions:
            return super().tests
        else:
            raise DbtInternalError(".tests for VersionedTestBlock with versions")

    @classmethod
    def from_yaml_block(cls, src: YamlBlock, target: Versioned) -> "VersionedTestBlock[Versioned]":
        return cls(
            file=src.file,
            data=src.data,
            target=target,
        )


@dataclass
class GenericTestBlock(TestBlock[Testable], Generic[Testable]):
    test: Dict[str, Any]
    column_name: Optional[str]
    tags: List[str]
    version: Optional[NodeVersion]

    @classmethod
    def from_test_block(
        cls,
        src: TestBlock,
        test: Dict[str, Any],
        column_name: Optional[str],
        tags: List[str],
        version: Optional[NodeVersion],
    ) -> "GenericTestBlock":
        return cls(
            file=src.file,
            data=src.data,
            target=src.target,
            test=test,
            column_name=column_name,
            tags=tags,
            version=version,
        )


class ParserRef:
    """A helper object to hold parse-time references."""

    def __init__(self):
        self.column_info: Dict[str, ColumnInfo] = {}

    def _add(self, column: HasColumnProps):
        tags: List[str] = []
        tags.extend(getattr(column, "tags", ()))
        quote: Optional[bool]
        if isinstance(column, UnparsedColumn):
            quote = column.quote
        else:
            quote = None

        if any(
            c
            for c in column.constraints
            if "type" not in c or not ConstraintType.is_valid(c["type"])
        ):
            raise ParsingError(f"Invalid constraint type on column {column.name}")

        self.column_info[column.name] = ColumnInfo(
            name=column.name,
            description=column.description,
            data_type=column.data_type,
            constraints=[ColumnLevelConstraint.from_dict(c) for c in column.constraints],
            meta=column.meta,
            tags=tags,
            quote=quote,
            _extra=column.extra,
        )

    @classmethod
    def from_target(cls, target: Union[HasColumnDocs, HasColumnTests]) -> "ParserRef":
        refs = cls()
        for column in target.columns:
            refs._add(column)
        return refs

    @classmethod
    def from_versioned_target(cls, target: Versioned, version: NodeVersion) -> "ParserRef":
        refs = cls()
        for base_column in target.get_columns_for_version(version):
            refs._add(base_column)
        return refs
