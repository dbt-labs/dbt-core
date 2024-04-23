from dataclasses import dataclass, field
from typing import Dict, Any, Sequence, List, Union, Optional
from datetime import datetime

from dbt.artifacts.resources import FreshnessThreshold
from dbt.artifacts.schemas.results import ExecutionResult, FreshnessStatus, NodeResult, TimingInfo
from dbt.artifacts.schemas.base import (
    ArtifactMixin,
    VersionedSchema,
    schema_version,
    BaseArtifactMetadata,
)
from dbt_common.dataclass_schema import dbtClassMixin, StrEnum
from dbt_common.exceptions import DbtInternalError

from dbt.contracts.graph.nodes import SourceDefinition


@dataclass
class SourceFreshnessResult(NodeResult):
    node: SourceDefinition
    status: FreshnessStatus
    max_loaded_at: datetime
    snapshotted_at: datetime
    age: float

    @property
    def skipped(self):
        return False


@dataclass
class PartialSourceFreshnessResult(NodeResult):
    status: FreshnessStatus

    @property
    def skipped(self):
        return False


FreshnessNodeResult = Union[PartialSourceFreshnessResult, SourceFreshnessResult]


@dataclass
class FreshnessMetadata(BaseArtifactMetadata):
    dbt_schema_version: str = field(
        default_factory=lambda: str(FreshnessExecutionResultArtifact.dbt_schema_version)
    )


@dataclass
class FreshnessResult(ExecutionResult):
    metadata: FreshnessMetadata
    results: Sequence[FreshnessNodeResult]

    @classmethod
    def from_node_results(
        cls,
        results: List[FreshnessNodeResult],
        elapsed_time: float,
        generated_at: datetime,
    ):
        meta = FreshnessMetadata(generated_at=generated_at)
        return cls(metadata=meta, results=results, elapsed_time=elapsed_time)

    def write(self, path):
        FreshnessExecutionResultArtifact.from_result(self).write(path)


@dataclass
class SourceFreshnessOutput(dbtClassMixin):
    unique_id: str
    max_loaded_at: datetime
    snapshotted_at: datetime
    max_loaded_at_time_ago_in_s: float
    status: FreshnessStatus
    criteria: FreshnessThreshold
    adapter_response: Dict[str, Any]
    timing: List[TimingInfo]
    thread_id: str
    execution_time: float


class FreshnessErrorEnum(StrEnum):
    runtime_error = "runtime error"


@dataclass
class SourceFreshnessRuntimeError(dbtClassMixin):
    unique_id: str
    error: Optional[Union[str, int]]
    status: FreshnessErrorEnum


FreshnessNodeOutput = Union[SourceFreshnessRuntimeError, SourceFreshnessOutput]


@dataclass
@schema_version("sources", 3)
class FreshnessExecutionResultArtifact(
    ArtifactMixin,
    VersionedSchema,
):
    metadata: FreshnessMetadata
    results: Sequence[FreshnessNodeOutput]
    elapsed_time: float

    @classmethod
    def from_result(cls, base: FreshnessResult):
        processed = [
            process_freshness_result(r)
            for r in base.results
            if isinstance(r, SourceFreshnessResult)
        ]
        return cls(
            metadata=base.metadata,
            results=processed,
            elapsed_time=base.elapsed_time,
        )


def process_freshness_result(result: FreshnessNodeResult) -> FreshnessNodeOutput:
    unique_id = result.node.unique_id
    if result.status == FreshnessStatus.RuntimeErr:
        return SourceFreshnessRuntimeError(
            unique_id=unique_id,
            error=result.message,
            status=FreshnessErrorEnum.runtime_error,
        )

    # we know that this must be a SourceFreshnessResult
    if not isinstance(result, SourceFreshnessResult):
        raise DbtInternalError(
            "Got {} instead of a SourceFreshnessResult for a "
            "non-error result in freshness execution!".format(type(result))
        )
    # if we're here, we must have a non-None freshness threshold
    criteria = result.node.freshness
    if criteria is None:
        raise DbtInternalError(
            "Somehow evaluated a freshness result for a source that has no freshness criteria!"
        )
    return SourceFreshnessOutput(
        unique_id=unique_id,
        max_loaded_at=result.max_loaded_at,
        snapshotted_at=result.snapshotted_at,
        max_loaded_at_time_ago_in_s=result.age,
        status=result.status,
        criteria=criteria,
        adapter_response=result.adapter_response,
        timing=result.timing,
        thread_id=result.thread_id,
        execution_time=result.execution_time,
    )
