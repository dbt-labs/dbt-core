# flake8: noqa

# This file is temporary, in order to not break various adapter tests, etc, until
# they are updated to use the new locations.

from dbt.artifacts.schemas.base import (
    ArtifactMixin,
    BaseArtifactMetadata,
    VersionedSchema,
    schema_version,
)
from dbt.artifacts.schemas.catalog import (
    CatalogArtifact,
    CatalogKey,
    CatalogMetadata,
    CatalogResults,
    CatalogTable,
    ColumnMetadata,
    StatsItem,
    TableMetadata,
)
from dbt.artifacts.schemas.freshness import (
    FreshnessErrorEnum,
    FreshnessExecutionResultArtifact,
    FreshnessMetadata,
    FreshnessNodeOutput,
    FreshnessNodeResult,
    FreshnessResult,
    PartialSourceFreshnessResult,
    SourceFreshnessOutput,
    SourceFreshnessResult,
    SourceFreshnessRuntimeError,
    process_freshness_result,
)
from dbt.artifacts.schemas.results import (
    BaseResult,
    ExecutionResult,
    FreshnessStatus,
    NodeResult,
    NodeStatus,
    RunningStatus,
    RunStatus,
    TestStatus,
    TimingInfo,
    collect_timing_info,
)
from dbt.artifacts.schemas.run import (
    RunExecutionResult,
    RunResult,
    RunResultsArtifact,
    RunResultsMetadata,
    process_run_result,
)
