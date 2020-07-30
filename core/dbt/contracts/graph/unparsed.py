from dbt.node_types import NodeType
from dbt.contracts.util import (
    AdditionalPropertiesMixin,
    Mergeable,
    Replaceable,
)
# trigger the PathEncoder
import dbt.helper_types  # noqa:F401
from dbt.exceptions import CompilationException

from hologram import JsonSchemaMixin
from hologram.helpers import StrEnum, ExtensibleJsonSchemaMixin

from dataclasses import dataclass, field
from datetime import timedelta
from pathlib import Path
from typing import Optional, List, Union, Dict, Any, Sequence


@dataclass
class UnparsedBaseNode(JsonSchemaMixin, Replaceable):
    package_name: str
    root_path: str
    path: str
    original_file_path: str


@dataclass
class HasSQL:
    raw_sql: str

    @property
    def empty(self):
        return not self.raw_sql.strip()


@dataclass
class UnparsedMacro(UnparsedBaseNode, HasSQL):
    resource_type: NodeType = field(metadata={'restrict': [NodeType.Macro]})


@dataclass
class UnparsedNode(UnparsedBaseNode, HasSQL):
    name: str
    resource_type: NodeType = field(metadata={'restrict': [
        NodeType.Model,
        NodeType.Analysis,
        NodeType.Test,
        NodeType.Snapshot,
        NodeType.Operation,
        NodeType.Seed,
        NodeType.RPCCall,
    ]})

    @property
    def search_name(self):
        return self.name


@dataclass
class UnparsedRunHook(UnparsedNode):
    resource_type: NodeType = field(
        metadata={'restrict': [NodeType.Operation]}
    )
    index: Optional[int] = None


@dataclass
class Docs(JsonSchemaMixin, Replaceable):
    show: bool = True


@dataclass
class HasDocs(AdditionalPropertiesMixin, ExtensibleJsonSchemaMixin,
              Replaceable):
    name: str
    description: str = ''
    meta: Dict[str, Any] = field(default_factory=dict)
    data_type: Optional[str] = None
    docs: Docs = field(default_factory=Docs)
    _extra: Dict[str, Any] = field(default_factory=dict)


TestDef = Union[Dict[str, Any], str]


@dataclass
class HasTests(HasDocs):
    tests: Optional[List[TestDef]] = None

    def __post_init__(self):
        if self.tests is None:
            self.tests = []


@dataclass
class UnparsedColumn(HasTests):
    quote: Optional[bool] = None
    tags: List[str] = field(default_factory=list)


@dataclass
class HasColumnDocs(JsonSchemaMixin, Replaceable):
    columns: Sequence[HasDocs] = field(default_factory=list)


@dataclass
class HasColumnTests(HasColumnDocs):
    columns: Sequence[UnparsedColumn] = field(default_factory=list)


@dataclass
class HasYamlMetadata(JsonSchemaMixin):
    original_file_path: str
    yaml_key: str
    package_name: str


@dataclass
class UnparsedAnalysisUpdate(HasColumnDocs, HasDocs, HasYamlMetadata):
    pass


@dataclass
class UnparsedNodeUpdate(HasColumnTests, HasTests, HasYamlMetadata):
    quote_columns: Optional[bool] = None


@dataclass
class MacroArgument(JsonSchemaMixin):
    name: str
    type: Optional[str] = None
    description: str = ''


@dataclass
class UnparsedMacroUpdate(HasDocs, HasYamlMetadata):
    arguments: List[MacroArgument] = field(default_factory=list)


class TimePeriod(StrEnum):
    minute = 'minute'
    hour = 'hour'
    day = 'day'

    def plural(self) -> str:
        return str(self) + 's'


@dataclass
class Time(JsonSchemaMixin, Replaceable):
    count: int
    period: TimePeriod

    def exceeded(self, actual_age: float) -> bool:
        kwargs = {self.period.plural(): self.count}
        difference = timedelta(**kwargs).total_seconds()
        return actual_age > difference


class FreshnessStatus(StrEnum):
    Pass = 'pass'
    Warn = 'warn'
    Error = 'error'


@dataclass
class FreshnessThreshold(JsonSchemaMixin, Mergeable):
    warn_after: Optional[Time] = None
    error_after: Optional[Time] = None
    filter: Optional[str] = None

    def status(self, age: float) -> FreshnessStatus:
        if self.error_after and self.error_after.exceeded(age):
            return FreshnessStatus.Error
        elif self.warn_after and self.warn_after.exceeded(age):
            return FreshnessStatus.Warn
        else:
            return FreshnessStatus.Pass

    def __bool__(self):
        return self.warn_after is not None or self.error_after is not None


@dataclass
class AdditionalPropertiesAllowed(
    AdditionalPropertiesMixin,
    ExtensibleJsonSchemaMixin
):
    _extra: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ExternalPartition(AdditionalPropertiesAllowed, Replaceable):
    name: str = ''
    description: str = ''
    data_type: str = ''
    meta: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self):
        if self.name == '' or self.data_type == '':
            raise CompilationException(
                'External partition columns must have names and data types'
            )


@dataclass
class ExternalTable(AdditionalPropertiesAllowed, Mergeable):
    location: Optional[str] = None
    file_format: Optional[str] = None
    row_format: Optional[str] = None
    tbl_properties: Optional[str] = None
    partitions: Optional[List[ExternalPartition]] = None

    def __bool__(self):
        return self.location is not None


@dataclass
class Quoting(JsonSchemaMixin, Mergeable):
    database: Optional[bool] = None
    schema: Optional[bool] = None
    identifier: Optional[bool] = None
    column: Optional[bool] = None


@dataclass
class UnparsedSourceTableDefinition(HasColumnTests, HasTests):
    loaded_at_field: Optional[str] = None
    identifier: Optional[str] = None
    quoting: Quoting = field(default_factory=Quoting)
    freshness: Optional[FreshnessThreshold] = field(
        default_factory=FreshnessThreshold
    )
    external: Optional[ExternalTable] = None
    tags: List[str] = field(default_factory=list)

    def to_dict(self, omit_none=True, validate=False):
        result = super().to_dict(omit_none=omit_none, validate=validate)
        if omit_none and self.freshness is None:
            result['freshness'] = None
        return result


@dataclass
class UnparsedSourceDefinition(JsonSchemaMixin, Replaceable):
    name: str
    description: str = ''
    meta: Dict[str, Any] = field(default_factory=dict)
    database: Optional[str] = None
    schema: Optional[str] = None
    loader: str = ''
    quoting: Quoting = field(default_factory=Quoting)
    freshness: Optional[FreshnessThreshold] = field(
        default_factory=FreshnessThreshold
    )
    loaded_at_field: Optional[str] = None
    tables: List[UnparsedSourceTableDefinition] = field(default_factory=list)
    tags: List[str] = field(default_factory=list)

    @property
    def yaml_key(self) -> 'str':
        return 'sources'

    def to_dict(self, omit_none=True, validate=False):
        result = super().to_dict(omit_none=omit_none, validate=validate)
        if omit_none and self.freshness is None:
            result['freshness'] = None
        return result


@dataclass
class SourceTablePatch(JsonSchemaMixin):
    name: str
    description: Optional[str] = None
    meta: Optional[Dict[str, Any]] = None
    data_type: Optional[str] = None
    docs: Optional[Docs] = None
    loaded_at_field: Optional[str] = None
    identifier: Optional[str] = None
    quoting: Quoting = field(default_factory=Quoting)
    freshness: Optional[FreshnessThreshold] = field(
        default_factory=FreshnessThreshold
    )
    external: Optional[ExternalTable] = None
    tags: Optional[List[str]] = None
    tests: Optional[List[TestDef]] = None
    columns: Optional[Sequence[UnparsedColumn]] = None

    def to_patch_dict(self) -> Dict[str, Any]:
        dct = self.to_dict(omit_none=True)
        remove_keys = ('name')
        for key in remove_keys:
            if key in dct:
                del dct[key]

        if self.freshness is None:
            dct['freshness'] = None

        return dct


@dataclass
class SourcePatch(JsonSchemaMixin, Replaceable):
    name: str = field(
        metadata=dict(description='The name of the source to override'),
    )
    overrides: str = field(
        metadata=dict(description='The package of the source to override'),
    )
    path: Path = field(
        metadata=dict(description='The path to the patch-defining yml file'),
    )
    description: Optional[str] = None
    meta: Optional[Dict[str, Any]] = None
    database: Optional[str] = None
    schema: Optional[str] = None
    loader: Optional[str] = None
    quoting: Optional[Quoting] = None
    freshness: Optional[Optional[FreshnessThreshold]] = field(
        default_factory=FreshnessThreshold
    )
    loaded_at_field: Optional[str] = None
    tables: Optional[List[SourceTablePatch]] = None
    tags: Optional[List[str]] = None

    def to_patch_dict(self) -> Dict[str, Any]:
        dct = self.to_dict(omit_none=True)
        remove_keys = ('name', 'overrides', 'tables', 'path')
        for key in remove_keys:
            if key in dct:
                del dct[key]

        if self.freshness is None:
            dct['freshness'] = None

        return dct

    def get_table_named(self, name: str) -> Optional[SourceTablePatch]:
        if self.tables is not None:
            for table in self.tables:
                if table.name == name:
                    return table
        return None


@dataclass
class UnparsedDocumentation(JsonSchemaMixin, Replaceable):
    package_name: str
    root_path: str
    path: str
    original_file_path: str

    @property
    def resource_type(self):
        return NodeType.Documentation


@dataclass
class UnparsedDocumentationFile(UnparsedDocumentation):
    file_contents: str
