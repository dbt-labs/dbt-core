import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Sequence

from dbt.artifacts.schemas.base import VersionedSchema, schema_version
from dbt.artifacts.schemas.results import ExecutionResult, TimingInfo
from dbt.artifacts.schemas.run import RunExecutionResult, RunResult, RunResultsArtifact
from dbt.contracts.graph.nodes import ResultNode
from dbt.events.types import ArtifactWritten
from dbt_common.dataclass_schema import dbtClassMixin
from dbt_common.events.functions import fire_event

TaskTags = Optional[Dict[str, Any]]
TaskID = uuid.UUID

# Outputs


@dataclass
class RemoteCompileResultMixin(VersionedSchema):
    raw_code: str
    compiled_code: str
    node: ResultNode
    timing: List[TimingInfo]


@dataclass
@schema_version("remote-compile-result", 1)
class RemoteCompileResult(RemoteCompileResultMixin):
    generated_at: datetime = field(
        default_factory=lambda: datetime.now(timezone.utc).replace(tzinfo=None)
    )

    @property
    def error(self) -> None:
        # TODO: Can we delete this? It's never set anywhere else and never accessed
        return None


@dataclass
@schema_version("remote-execution-result", 1)
class RemoteExecutionResult(ExecutionResult):
    results: Sequence[RunResult]
    args: Dict[str, Any] = field(default_factory=dict)
    generated_at: datetime = field(
        default_factory=lambda: datetime.now(timezone.utc).replace(tzinfo=None)
    )

    def write(self, path: str) -> None:
        writable = RunResultsArtifact.from_execution_results(
            generated_at=self.generated_at,
            results=self.results,
            elapsed_time=self.elapsed_time,
            args=self.args,
        )
        writable.write(path)
        fire_event(ArtifactWritten(artifact_type=writable.__class__.__name__, artifact_path=path))

    @classmethod
    def from_local_result(
        cls,
        base: RunExecutionResult,
    ) -> "RemoteExecutionResult":
        return cls(
            generated_at=base.generated_at,
            results=base.results,
            elapsed_time=base.elapsed_time,
            args=base.args,
        )


@dataclass
class ResultTable(dbtClassMixin):
    column_names: List[str]
    rows: List[Any]


@dataclass
@schema_version("remote-run-result", 1)
class RemoteRunResult(RemoteCompileResultMixin):
    table: ResultTable
    generated_at: datetime = field(
        default_factory=lambda: datetime.now(timezone.utc).replace(tzinfo=None)
    )
