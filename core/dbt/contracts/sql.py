import uuid
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional, List, Any, Dict, Sequence

from dbt.dataclass_schema import dbtClassMixin

from dbt.contracts.graph.compiled import CompileResultNode
from dbt.contracts.results import (
    RunResult, RunResultsArtifact, TimingInfo,
    ExecutionResult,
    RunExecutionResult,
)
from dbt.contracts.util import VersionedSchema, schema_version
from dbt.logger import LogMessage


TaskTags = Optional[Dict[str, Any]]
TaskID = uuid.UUID

# Outputs


@dataclass
class RemoteResult(VersionedSchema):
    logs: List[LogMessage]


@dataclass
class RemoteCompileResultMixin(RemoteResult):
    raw_sql: str
    compiled_sql: str
    node: CompileResultNode
    timing: List[TimingInfo]


@dataclass
@schema_version('remote-compile-result', 1)
class RemoteCompileResult(RemoteCompileResultMixin):
    generated_at: datetime = field(default_factory=datetime.utcnow)

    @property
    def error(self):
        return None


@dataclass
@schema_version('remote-execution-result', 1)
class RemoteExecutionResult(ExecutionResult, RemoteResult):
    results: Sequence[RunResult]
    args: Dict[str, Any] = field(default_factory=dict)
    generated_at: datetime = field(default_factory=datetime.utcnow)

    def write(self, path: str):
        writable = RunResultsArtifact.from_execution_results(
            generated_at=self.generated_at,
            results=self.results,
            elapsed_time=self.elapsed_time,
            args=self.args,
        )
        writable.write(path)

    @classmethod
    def from_local_result(
        cls,
        base: RunExecutionResult,
        logs: List[LogMessage],
    ) -> 'RemoteExecutionResult':
        return cls(
            generated_at=base.generated_at,
            results=base.results,
            elapsed_time=base.elapsed_time,
            args=base.args,
            logs=logs,
        )


@dataclass
class ResultTable(dbtClassMixin):
    column_names: List[str]
    rows: List[Any]


@dataclass
@schema_version('remote-run-result', 1)
class RemoteRunResult(RemoteCompileResultMixin):
    table: ResultTable
    generated_at: datetime = field(default_factory=datetime.utcnow)
