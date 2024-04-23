from dbt.artifacts.resources.base import BaseResource, GraphResource, FileHash, Docs

# alias to latest resource definitions
from dbt.artifacts.resources.v1.components import (
    DependsOn,
    NodeVersion,
    RefArgs,
    HasRelationMetadata,
    ParsedResourceMandatory,
    ParsedResource,
    ColumnInfo,
    CompiledResource,
    InjectedCTE,
    Contract,
    DeferRelation,
    FreshnessThreshold,
    Quoting,
    Time,
)
from dbt.artifacts.resources.v1.analysis import Analysis
from dbt.artifacts.resources.v1.hook import HookNode
from dbt.artifacts.resources.v1.model import Model, ModelConfig
from dbt.artifacts.resources.v1.sql_operation import SqlOperation
from dbt.artifacts.resources.v1.seed import Seed, SeedConfig
from dbt.artifacts.resources.v1.singular_test import SingularTest
from dbt.artifacts.resources.v1.generic_test import GenericTest, TestMetadata
from dbt.artifacts.resources.v1.snapshot import Snapshot, SnapshotConfig


from dbt.artifacts.resources.v1.documentation import Documentation
from dbt.artifacts.resources.v1.exposure import (
    Exposure,
    ExposureConfig,
    ExposureType,
    MaturityType,
)
from dbt.artifacts.resources.v1.macro import Macro, MacroDependsOn, MacroArgument
from dbt.artifacts.resources.v1.group import Group
from dbt.artifacts.resources.v1.metric import (
    ConstantPropertyInput,
    ConversionTypeParams,
    Metric,
    MetricConfig,
    MetricInput,
    MetricInputMeasure,
    MetricTimeWindow,
    MetricTypeParams,
)
from dbt.artifacts.resources.v1.owner import Owner
from dbt.artifacts.resources.v1.saved_query import (
    Export,
    ExportConfig,
    QueryParams,
    SavedQuery,
    SavedQueryConfig,
    SavedQueryMandatory,
)
from dbt.artifacts.resources.v1.semantic_layer_components import (
    FileSlice,
    SourceFileMetadata,
    WhereFilter,
    WhereFilterIntersection,
)
from dbt.artifacts.resources.v1.semantic_model import (
    Defaults,
    Dimension,
    DimensionTypeParams,
    DimensionValidityParams,
    Entity,
    Measure,
    MeasureAggregationParameters,
    NodeRelation,
    NonAdditiveDimension,
    SemanticModel,
    SemanticModelConfig,
)

from dbt.artifacts.resources.v1.config import (
    NodeAndTestConfig,
    NodeConfig,
    TestConfig,
    Hook,
)

from dbt.artifacts.resources.v1.source_definition import (
    SourceConfig,
    ExternalPartition,
    ExternalTable,
    SourceDefinition,
    ParsedSourceMandatory,
)

from dbt.artifacts.resources.v1.unit_test_definition import (
    UnitTestConfig,
    UnitTestDefinition,
    UnitTestInputFixture,
    UnitTestOutputFixture,
    UnitTestOverrides,
    UnitTestNodeVersions,
    UnitTestFormat,
)
