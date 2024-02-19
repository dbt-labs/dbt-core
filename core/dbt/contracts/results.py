# flake8: noqa

# This file is temporary, in order to not break various adapter tests, etc, until
# they are updated to use the new locations.

from dbt.artifacts.schemas.base import (
    ArtifactMixin,
    BaseArtifactMetadata,
    VersionedSchema,
    schema_version,
)

from dbt.artifacts.schemas.results import (
    NodeStatus,
    RunStatus,
    TestStatus,
    FreshnessStatus,
    RunningStatus,
    TimingInfo,
    collect_timing_info,
    BaseResult,
    NodeResult,
    ExecutionResult,
)

from dbt.artifacts.schemas.run import (
    RunResult,
    RunResultsMetadata,
    RunExecutionResult,
    RunResultsArtifact,
    process_run_result,
)

from dbt.artifacts.schemas.freshness import (
    FreshnessErrorEnum,
    FreshnessMetadata,
    FreshnessResult,
    FreshnessExecutionResultArtifact,
    FreshnessNodeResult,
    FreshnessNodeOutput,
    process_freshness_result,
    SourceFreshnessResult,
    SourceFreshnessRuntimeError,
    SourceFreshnessOutput,
    PartialSourceFreshnessResult,
)

from dbt.artifacts.schemas.catalog import (
    CatalogResults,
    CatalogKey,
    StatsItem,
    ColumnMetadata,
    TableMetadata,
    CatalogTable,
    CatalogMetadata,
    CatalogResults,
    CatalogArtifact,
)
