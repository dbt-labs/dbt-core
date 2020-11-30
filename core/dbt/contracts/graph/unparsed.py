from dbt.node_types import NodeType
from dbt.contracts.util import (
    AdditionalPropertiesMixin,
    Mergeable,
    Replaceable,
)

# trigger the PathEncoder
import dbt.helper_types  # noqa:F401
from dbt.exceptions import CompilationException

from dbt.dataclass_schema import dbtClassMixin, StrEnum, ExtensibleDbtClassMixin

from dataclasses import dataclass, field
from datetime import timedelta
from pathlib import Path
from typing import Optional, List, Union, Dict, Any, Sequence


@dataclass
class UnparsedBaseNode(dbtClassMixin, Replaceable):
    package_name: str
    root_path: str
    path: str
    original_file_path: str

    @property
    def file_id(self):
        return f"{self.package_name}://{self.original_file_path}"


@dataclass
class HasSQL:
    raw_sql: str

    @property
    def empty(self):
        return not self.raw_sql.strip()


@dataclass
class UnparsedMacro(UnparsedBaseNode, HasSQL):
    resource_type: NodeType = field(metadata={"restrict": [NodeType.Macro]})


@dataclass
class UnparsedGenericTest(UnparsedBaseNode, HasSQL):
    resource_type: NodeType = field(metadata={"restrict": [NodeType.Macro]})


@dataclass
class UnparsedNode(UnparsedBaseNode, HasSQL):
    name: str
    resource_type: NodeType = field(
        metadata={
            "restrict": [
                NodeType.Model,
                NodeType.Analysis,
                NodeType.Test,
                NodeType.Snapshot,
                NodeType.Operation,
                NodeType.Seed,
                NodeType.RPCCall,
                NodeType.SqlOperation,
            ]
        }
    )

    @property
    def search_name(self):
        return self.name


@dataclass
class UnparsedRunHook(UnparsedNode):
    resource_type: NodeType = field(metadata={"restrict": [NodeType.Operation]})
    index: Optional[int] = None


@dataclass
class Docs(dbtClassMixin, Replaceable):
    show: bool = True


@dataclass
class HasDocs(AdditionalPropertiesMixin, ExtensibleDbtClassMixin, Replaceable):
    name: str
    description: str = ""
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
class HasColumnDocs(dbtClassMixin, Replaceable):
    columns: Sequence[HasDocs] = field(default_factory=list)


@dataclass
class HasColumnTests(HasColumnDocs):
    columns: Sequence[UnparsedColumn] = field(default_factory=list)


@dataclass
class HasYamlMetadata(dbtClassMixin):
    original_file_path: str
    yaml_key: str
    package_name: str

    @property
    def file_id(self):
        return f"{self.package_name}://{self.original_file_path}"


@dataclass
class HasConfig:
    config: Dict[str, Any] = field(default_factory=dict)


@dataclass
class UnparsedAnalysisUpdate(HasConfig, HasColumnDocs, HasDocs, HasYamlMetadata):
    pass


@dataclass
class UnparsedNodeUpdate(HasConfig, HasColumnTests, HasTests, HasYamlMetadata):
    quote_columns: Optional[bool] = None


@dataclass
class MacroArgument(dbtClassMixin):
    name: str
    type: Optional[str] = None
    description: str = ""


@dataclass
class UnparsedMacroUpdate(HasConfig, HasDocs, HasYamlMetadata):
    arguments: List[MacroArgument] = field(default_factory=list)


class TimePeriod(StrEnum):
    minute = "minute"
    hour = "hour"
    day = "day"

    def plural(self) -> str:
        return str(self) + "s"


@dataclass
class Time(dbtClassMixin, Mergeable):
    count: Optional[int] = None
    period: Optional[TimePeriod] = None

    def exceeded(self, actual_age: float) -> bool:
        if self.period is None or self.count is None:
            return False
        kwargs: Dict[str, int] = {self.period.plural(): self.count}
        difference = timedelta(**kwargs).total_seconds()
        return actual_age > difference

    def __bool__(self):
        return self.count is not None and self.period is not None


@dataclass
class FreshnessThreshold(dbtClassMixin, Mergeable):
    warn_after: Optional[Time] = field(default_factory=Time)
    error_after: Optional[Time] = field(default_factory=Time)
    filter: Optional[str] = None

    def status(self, age: float) -> "dbt.contracts.results.FreshnessStatus":
        from dbt.contracts.results import FreshnessStatus

        if self.error_after and self.error_after.exceeded(age):
            return FreshnessStatus.Error
        elif self.warn_after and self.warn_after.exceeded(age):
            return FreshnessStatus.Warn
        else:
            return FreshnessStatus.Pass

    def __bool__(self):
        return bool(self.warn_after) or bool(self.error_after)


@dataclass
class AdditionalPropertiesAllowed(AdditionalPropertiesMixin, ExtensibleDbtClassMixin):
    _extra: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ExternalPartition(AdditionalPropertiesAllowed, Replaceable):
    name: str = ""
    description: str = ""
    data_type: str = ""
    meta: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self):
        if self.name == "" or self.data_type == "":
            raise CompilationException("External partition columns must have names and data types")


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
class Quoting(dbtClassMixin, Mergeable):
    database: Optional[bool] = None
    schema: Optional[bool] = None
    identifier: Optional[bool] = None
    column: Optional[bool] = None


@dataclass
class UnparsedSourceTableDefinition(HasColumnTests, HasTests):
    loaded_at_field: Optional[str] = None
    identifier: Optional[str] = None
    quoting: Quoting = field(default_factory=Quoting)
    freshness: Optional[FreshnessThreshold] = field(default_factory=FreshnessThreshold)
    external: Optional[ExternalTable] = None
    tags: List[str] = field(default_factory=list)

    def __post_serialize__(self, dct):
        dct = super().__post_serialize__(dct)
        if "freshness" not in dct and self.freshness is None:
            dct["freshness"] = None
        return dct


@dataclass
class UnparsedSourceDefinition(dbtClassMixin, Replaceable):
    name: str
    description: str = ""
    meta: Dict[str, Any] = field(default_factory=dict)
    database: Optional[str] = None
    schema: Optional[str] = None
    loader: str = ""
    quoting: Quoting = field(default_factory=Quoting)
    freshness: Optional[FreshnessThreshold] = field(default_factory=FreshnessThreshold)
    loaded_at_field: Optional[str] = None
    tables: List[UnparsedSourceTableDefinition] = field(default_factory=list)
    tags: List[str] = field(default_factory=list)
    config: Dict[str, Any] = field(default_factory=dict)

    @property
    def yaml_key(self) -> "str":
        return "sources"

    def __post_serialize__(self, dct):
        dct = super().__post_serialize__(dct)
        if "freshness" not in dct and self.freshness is None:
            dct["freshness"] = None
        return dct


@dataclass
class SourceTablePatch(dbtClassMixin):
    name: str
    description: Optional[str] = None
    meta: Optional[Dict[str, Any]] = None
    data_type: Optional[str] = None
    docs: Optional[Docs] = None
    loaded_at_field: Optional[str] = None
    identifier: Optional[str] = None
    quoting: Quoting = field(default_factory=Quoting)
    freshness: Optional[FreshnessThreshold] = field(default_factory=FreshnessThreshold)
    external: Optional[ExternalTable] = None
    tags: Optional[List[str]] = None
    tests: Optional[List[TestDef]] = None
    columns: Optional[Sequence[UnparsedColumn]] = None

    def to_patch_dict(self) -> Dict[str, Any]:
        dct = self.to_dict(omit_none=True)
        remove_keys = "name"
        for key in remove_keys:
            if key in dct:
                del dct[key]

        if self.freshness is None:
            dct["freshness"] = None

        return dct


@dataclass
class SourcePatch(dbtClassMixin, Replaceable):
    name: str = field(
        metadata=dict(description="The name of the source to override"),
    )
    overrides: str = field(
        metadata=dict(description="The package of the source to override"),
    )
    path: Path = field(
        metadata=dict(description="The path to the patch-defining yml file"),
    )
    description: Optional[str] = None
    meta: Optional[Dict[str, Any]] = None
    database: Optional[str] = None
    schema: Optional[str] = None
    loader: Optional[str] = None
    quoting: Optional[Quoting] = None
    freshness: Optional[Optional[FreshnessThreshold]] = field(default_factory=FreshnessThreshold)
    loaded_at_field: Optional[str] = None
    tables: Optional[List[SourceTablePatch]] = None
    tags: Optional[List[str]] = None

    def to_patch_dict(self) -> Dict[str, Any]:
        dct = self.to_dict(omit_none=True)
        remove_keys = ("name", "overrides", "tables", "path")
        for key in remove_keys:
            if key in dct:
                del dct[key]

        if self.freshness is None:
            dct["freshness"] = None

        return dct

    def get_table_named(self, name: str) -> Optional[SourceTablePatch]:
        if self.tables is not None:
            for table in self.tables:
                if table.name == name:
                    return table
        return None


@dataclass
class UnparsedDocumentation(dbtClassMixin, Replaceable):
    package_name: str
    root_path: str
    path: str
    original_file_path: str

    @property
    def file_id(self):
        return f"{self.package_name}://{self.original_file_path}"

    @property
    def resource_type(self):
        return NodeType.Documentation


@dataclass
class UnparsedDocumentationFile(UnparsedDocumentation):
    file_contents: str


# can't use total_ordering decorator here, as str provides an ordering already
# and it's not the one we want.
class Maturity(StrEnum):
    low = "low"
    medium = "medium"
    high = "high"

    def __lt__(self, other):
        if not isinstance(other, Maturity):
            return NotImplemented
        order = (Maturity.low, Maturity.medium, Maturity.high)
        return order.index(self) < order.index(other)

    def __gt__(self, other):
        if not isinstance(other, Maturity):
            return NotImplemented
        return self != other and not (self < other)

    def __ge__(self, other):
        if not isinstance(other, Maturity):
            return NotImplemented
        return self == other or not (self < other)

    def __le__(self, other):
        if not isinstance(other, Maturity):
            return NotImplemented
        return self == other or self < other


class ExposureType(StrEnum):
    Dashboard = "dashboard"
    Notebook = "notebook"
    Analysis = "analysis"
    ML = "ml"
    Application = "application"


class MaturityType(StrEnum):
    Low = "low"
    Medium = "medium"
    High = "high"


@dataclass
class ExposureOwner(dbtClassMixin, Replaceable):
    email: str
    name: Optional[str] = None


@dataclass
class UnparsedExposure(dbtClassMixin, Replaceable):
    name: str
    type: ExposureType
    owner: ExposureOwner
    description: str = ""
    maturity: Optional[MaturityType] = None
    meta: Dict[str, Any] = field(default_factory=dict)
    tags: List[str] = field(default_factory=list)
    url: Optional[str] = None
    depends_on: List[str] = field(default_factory=list)


@dataclass
class MetricFilter(dbtClassMixin, Replaceable):
    field: str
    operator: str
    # TODO : Can we make this Any?
    value: str


@dataclass
class UnparsedMetric(dbtClassMixin, Replaceable):
    model: str
    name: str
    label: str
    type: str
    description: str = ""
    sql: Optional[str] = None
    timestamp: Optional[str] = None
    time_grains: List[str] = field(default_factory=list)
    dimensions: List[str] = field(default_factory=list)
    filters: List[MetricFilter] = field(default_factory=list)
    meta: Dict[str, Any] = field(default_factory=dict)
    tags: List[str] = field(default_factory=list)
