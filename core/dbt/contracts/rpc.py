import enum
import os
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Optional, Union, List, Any, Dict, Type, Sequence

from dbt.dataclass_schema import dbtClassMixin, StrEnum

from dbt.contracts.graph.compiled import CompileResultNode
from dbt.contracts.graph.manifest import WritableManifest
from dbt.contracts.results import (
    RunResult, RunResultsArtifact, TimingInfo,
    CatalogArtifact,
    CatalogResults,
    ExecutionResult,
    FreshnessExecutionResultArtifact,
    FreshnessResult,
    RunOperationResult,
    RunOperationResultsArtifact,
    RunExecutionResult,
)
from dbt.contracts.util import VersionedSchema, schema_version
from dbt.exceptions import InternalException
from dbt.logger import LogMessage
from dbt.utils import restrict_to


TaskTags = Optional[Dict[str, Any]]
TaskID = uuid.UUID

# Inputs


@dataclass
class RPCParameters(dbtClassMixin):
    task_tags: TaskTags
    timeout: Optional[float]

    @classmethod
    def __pre_deserialize__(cls, data, omit_none=True):
        data = super().__pre_deserialize__(data)
        if 'timeout' not in data:
            data['timeout'] = None
        if 'task_tags' not in data:
            data['task_tags'] = None
        return data


@dataclass
class RPCExecParameters(RPCParameters):
    name: str
    sql: str
    macros: Optional[str] = None


@dataclass
class RPCCompileParameters(RPCParameters):
    threads: Optional[int] = None
    models: Union[None, str, List[str]] = None
    select: Union[None, str, List[str]] = None
    exclude: Union[None, str, List[str]] = None
    selector: Optional[str] = None
    state: Optional[str] = None


@dataclass
class RPCListParameters(RPCParameters):
    resource_types: Optional[List[str]] = None
    models: Union[None, str, List[str]] = None
    exclude: Union[None, str, List[str]] = None
    select: Union[None, str, List[str]] = None
    selector: Optional[str] = None
    output: Optional[str] = 'json'
    output_keys: Optional[List[str]] = None


@dataclass
class RPCRunParameters(RPCParameters):
    threads: Optional[int] = None
    models: Union[None, str, List[str]] = None
    select: Union[None, str, List[str]] = None
    exclude: Union[None, str, List[str]] = None
    selector: Optional[str] = None
    state: Optional[str] = None
    defer: Optional[bool] = None


@dataclass
class RPCSnapshotParameters(RPCParameters):
    threads: Optional[int] = None
    select: Union[None, str, List[str]] = None
    exclude: Union[None, str, List[str]] = None
    selector: Optional[str] = None
    state: Optional[str] = None


@dataclass
class RPCTestParameters(RPCCompileParameters):
    data: bool = False
    schema: bool = False
    state: Optional[str] = None
    defer: Optional[bool] = None


@dataclass
class RPCSeedParameters(RPCParameters):
    threads: Optional[int] = None
    select: Union[None, str, List[str]] = None
    exclude: Union[None, str, List[str]] = None
    selector: Optional[str] = None
    show: bool = False
    state: Optional[str] = None


@dataclass
class RPCDocsGenerateParameters(RPCParameters):
    compile: bool = True
    state: Optional[str] = None


@dataclass
class RPCBuildParameters(RPCParameters):
    resource_types: Optional[List[str]] = None
    select: Union[None, str, List[str]] = None
    threads: Optional[int] = None
    exclude: Union[None, str, List[str]] = None
    selector: Optional[str] = None
    state: Optional[str] = None
    defer: Optional[bool] = None


@dataclass
class RPCCliParameters(RPCParameters):
    cli: str


@dataclass
class RPCDepsParameters(RPCParameters):
    pass


@dataclass
class KillParameters(RPCParameters):
    task_id: TaskID


@dataclass
class PollParameters(RPCParameters):
    request_token: TaskID
    logs: bool = True
    logs_start: int = 0


@dataclass
class PSParameters(RPCParameters):
    active: bool = True
    completed: bool = False


@dataclass
class StatusParameters(RPCParameters):
    pass


@dataclass
class GCSettings(dbtClassMixin):
    # start evicting the longest-ago-ended tasks here
    maxsize: int
    # start evicting all tasks before now - auto_reap_age when we have this
    # many tasks in the table
    reapsize: int
    # a positive timedelta indicating how far back we should go
    auto_reap_age: timedelta


@dataclass
class GCParameters(RPCParameters):
    """The gc endpoint takes three arguments, any of which may be present:

    - task_ids: An optional list of task ID UUIDs to try to GC
    - before: If provided, should be a datetime string. All tasks that finished
        before that datetime will be GCed
    - settings: If provided, should be a GCSettings object in JSON form. It
        will be applied to the task manager before GC starts. By default the
        existing gc settings remain.
    """
    task_ids: Optional[List[TaskID]] = None
    before: Optional[datetime] = None
    settings: Optional[GCSettings] = None


@dataclass
class RPCRunOperationParameters(RPCParameters):
    macro: str
    args: Dict[str, Any] = field(default_factory=dict)


@dataclass
class RPCSourceFreshnessParameters(RPCParameters):
    threads: Optional[int] = None
    select: Union[None, str, List[str]] = None
    exclude: Union[None, str, List[str]] = None
    selector: Optional[str] = None


@dataclass
class GetManifestParameters(RPCParameters):
    pass

# Outputs


@dataclass
class RemoteResult(VersionedSchema):
    logs: List[LogMessage]


@dataclass
@schema_version('remote-list-results', 1)
class RemoteListResults(RemoteResult):
    output: List[Any]
    generated_at: datetime = field(default_factory=datetime.utcnow)


@dataclass
@schema_version('remote-deps-result', 1)
class RemoteDepsResult(RemoteResult):
    generated_at: datetime = field(default_factory=datetime.utcnow)


@dataclass
@schema_version('remote-catalog-result', 1)
class RemoteCatalogResults(CatalogResults, RemoteResult):
    generated_at: datetime = field(default_factory=datetime.utcnow)

    def write(self, path: str):
        artifact = CatalogArtifact.from_results(
            generated_at=self.generated_at,
            nodes=self.nodes,
            sources=self.sources,
            compile_results=self._compile_results,
            errors=self.errors,
        )
        artifact.write(path)


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
@schema_version('remote-run-operation-result', 1)
class RemoteRunOperationResult(RunOperationResult, RemoteResult):
    generated_at: datetime = field(default_factory=datetime.utcnow)

    @classmethod
    def from_local_result(
        cls,
        base: RunOperationResultsArtifact,
        logs: List[LogMessage],
    ) -> 'RemoteRunOperationResult':
        return cls(
            generated_at=base.metadata.generated_at,
            results=base.results,
            elapsed_time=base.elapsed_time,
            success=base.success,
            logs=logs,
        )

    def write(self, path: str):
        writable = RunOperationResultsArtifact.from_success(
            success=self.success,
            generated_at=self.generated_at,
            elapsed_time=self.elapsed_time,
        )
        writable.write(path)


@dataclass
@schema_version('remote-freshness-result', 1)
class RemoteFreshnessResult(FreshnessResult, RemoteResult):

    @classmethod
    def from_local_result(
        cls,
        base: FreshnessResult,
        logs: List[LogMessage],
    ) -> 'RemoteFreshnessResult':
        return cls(
            metadata=base.metadata,
            results=base.results,
            elapsed_time=base.elapsed_time,
            logs=logs,
        )

    def write(self, path: str):
        writable = FreshnessExecutionResultArtifact.from_result(base=self)
        writable.write(path)


@dataclass
@schema_version('remote-run-result', 1)
class RemoteRunResult(RemoteCompileResultMixin):
    table: ResultTable
    generated_at: datetime = field(default_factory=datetime.utcnow)


RPCResult = Union[
    RemoteCompileResult,
    RemoteExecutionResult,
    RemoteFreshnessResult,
    RemoteCatalogResults,
    RemoteDepsResult,
    RemoteRunOperationResult,
]


# GC types

class GCResultState(StrEnum):
    Deleted = 'deleted'  # successful GC
    Missing = 'missing'  # nothing to GC
    Running = 'running'  # can't GC


@dataclass
@schema_version('remote-gc-result', 1)
class GCResult(RemoteResult):
    logs: List[LogMessage] = field(default_factory=list)
    deleted: List[TaskID] = field(default_factory=list)
    missing: List[TaskID] = field(default_factory=list)
    running: List[TaskID] = field(default_factory=list)

    def add_result(self, task_id: TaskID, state: GCResultState):
        if state == GCResultState.Missing:
            self.missing.append(task_id)
        elif state == GCResultState.Running:
            self.running.append(task_id)
        elif state == GCResultState.Deleted:
            self.deleted.append(task_id)
        else:
            raise InternalException(
                f'Got invalid state in add_result: {state}'
            )

# Task management types


class TaskHandlerState(StrEnum):
    NotStarted = 'not started'
    Initializing = 'initializing'
    Running = 'running'
    Success = 'success'
    Error = 'error'
    Killed = 'killed'
    Failed = 'failed'

    def __lt__(self, other) -> bool:
        """A logical ordering for TaskHandlerState:

        NotStarted < Initializing < Running < (Success, Error, Killed, Failed)
        """
        if not isinstance(other, TaskHandlerState):
            raise TypeError('cannot compare to non-TaskHandlerState')
        order = (self.NotStarted, self.Initializing, self.Running)
        smaller = set()
        for value in order:
            smaller.add(value)
            if self == value:
                return other not in smaller

        return False

    def __le__(self, other) -> bool:
        # so that ((Success <= Error) is True)
        return ((self < other) or
                (self == other) or
                (self.finished and other.finished))

    def __gt__(self, other) -> bool:
        if not isinstance(other, TaskHandlerState):
            raise TypeError('cannot compare to non-TaskHandlerState')
        order = (self.NotStarted, self.Initializing, self.Running)
        smaller = set()
        for value in order:
            smaller.add(value)
            if self == value:
                return other in smaller
        return other in smaller

    def __ge__(self, other) -> bool:
        # so that ((Success <= Error) is True)
        return ((self > other) or
                (self == other) or
                (self.finished and other.finished))

    @property
    def finished(self) -> bool:
        return self in (self.Error, self.Success, self.Killed, self.Failed)


@dataclass
class TaskTiming(dbtClassMixin):
    state: TaskHandlerState
    start: Optional[datetime]
    end: Optional[datetime]
    elapsed: Optional[float]

    # These ought to be defaults but superclass order doesn't
    # allow that to work
    @classmethod
    def __pre_deserialize__(cls, data):
        data = super().__pre_deserialize__(data)
        for field_name in ('start', 'end', 'elapsed'):
            if field_name not in data:
                data[field_name] = None
        return data


@dataclass
class TaskRow(TaskTiming):
    task_id: TaskID
    request_source: str
    method: str
    request_id: Union[str, int]
    tags: TaskTags = None
    timeout: Optional[float] = None


@dataclass
@schema_version('remote-ps-result', 1)
class PSResult(RemoteResult):
    rows: List[TaskRow]


class KillResultStatus(StrEnum):
    Missing = 'missing'
    NotStarted = 'not_started'
    Killed = 'killed'
    Finished = 'finished'


@dataclass
@schema_version('remote-kill-result', 1)
class KillResult(RemoteResult):
    state: KillResultStatus = KillResultStatus.Missing
    logs: List[LogMessage] = field(default_factory=list)


@dataclass
@schema_version('remote-manifest-result', 1)
class GetManifestResult(RemoteResult):
    manifest: Optional[WritableManifest] = None


# this is kind of carefuly structured: BlocksManifestTasks is implied by
# RequiresConfigReloadBefore and RequiresManifestReloadAfter
class RemoteMethodFlags(enum.Flag):
    Empty = 0
    BlocksManifestTasks = 1
    RequiresConfigReloadBefore = 3
    RequiresManifestReloadAfter = 5
    Builtin = 8


# Polling types


@dataclass
class PollResult(RemoteResult, TaskTiming):
    state: TaskHandlerState
    tags: TaskTags
    start: Optional[datetime]
    end: Optional[datetime]
    elapsed: Optional[float]

    # These ought to be defaults but superclass order doesn't
    # allow that to work
    @classmethod
    def __pre_deserialize__(cls, data):
        data = super().__pre_deserialize__(data)
        for field_name in ('start', 'end', 'elapsed'):
            if field_name not in data:
                data[field_name] = None
        return data


@dataclass
@schema_version('poll-remote-deps-result', 1)
class PollRemoteEmptyCompleteResult(PollResult, RemoteResult):
    state: TaskHandlerState = field(
        metadata=restrict_to(TaskHandlerState.Success,
                             TaskHandlerState.Failed),
    )
    generated_at: datetime = field(default_factory=datetime.utcnow)

    @classmethod
    def from_result(
        cls: Type['PollRemoteEmptyCompleteResult'],
        base: RemoteDepsResult,
        tags: TaskTags,
        timing: TaskTiming,
        logs: List[LogMessage],
    ) -> 'PollRemoteEmptyCompleteResult':
        return cls(
            logs=logs,
            tags=tags,
            state=timing.state,
            start=timing.start,
            end=timing.end,
            elapsed=timing.elapsed,
            generated_at=base.generated_at
        )


@dataclass
@schema_version('poll-remote-killed-result', 1)
class PollKilledResult(PollResult):
    state: TaskHandlerState = field(
        metadata=restrict_to(TaskHandlerState.Killed),
    )


@dataclass
@schema_version('poll-remote-execution-result', 1)
class PollExecuteCompleteResult(
    RemoteExecutionResult,
    PollResult,
):
    state: TaskHandlerState = field(
        metadata=restrict_to(TaskHandlerState.Success,
                             TaskHandlerState.Failed),
    )

    @classmethod
    def from_result(
        cls: Type['PollExecuteCompleteResult'],
        base: RemoteExecutionResult,
        tags: TaskTags,
        timing: TaskTiming,
        logs: List[LogMessage],
    ) -> 'PollExecuteCompleteResult':
        return cls(
            results=base.results,
            elapsed_time=base.elapsed_time,
            logs=logs,
            tags=tags,
            state=timing.state,
            start=timing.start,
            end=timing.end,
            elapsed=timing.elapsed,
            generated_at=base.generated_at,
        )


@dataclass
@schema_version('poll-remote-compile-result', 1)
class PollCompileCompleteResult(
    RemoteCompileResult,
    PollResult,
):
    state: TaskHandlerState = field(
        metadata=restrict_to(TaskHandlerState.Success,
                             TaskHandlerState.Failed),
    )

    @classmethod
    def from_result(
        cls: Type['PollCompileCompleteResult'],
        base: RemoteCompileResult,
        tags: TaskTags,
        timing: TaskTiming,
        logs: List[LogMessage],
    ) -> 'PollCompileCompleteResult':
        return cls(
            raw_sql=base.raw_sql,
            compiled_sql=base.compiled_sql,
            node=base.node,
            timing=base.timing,
            logs=logs,
            tags=tags,
            state=timing.state,
            start=timing.start,
            end=timing.end,
            elapsed=timing.elapsed,
            generated_at=base.generated_at
        )


@dataclass
@schema_version('poll-remote-run-result', 1)
class PollRunCompleteResult(
    RemoteRunResult,
    PollResult,
):
    state: TaskHandlerState = field(
        metadata=restrict_to(TaskHandlerState.Success,
                             TaskHandlerState.Failed),
    )

    @classmethod
    def from_result(
        cls: Type['PollRunCompleteResult'],
        base: RemoteRunResult,
        tags: TaskTags,
        timing: TaskTiming,
        logs: List[LogMessage],
    ) -> 'PollRunCompleteResult':
        return cls(
            raw_sql=base.raw_sql,
            compiled_sql=base.compiled_sql,
            node=base.node,
            timing=base.timing,
            logs=logs,
            table=base.table,
            tags=tags,
            state=timing.state,
            start=timing.start,
            end=timing.end,
            elapsed=timing.elapsed,
            generated_at=base.generated_at
        )


@dataclass
@schema_version('poll-remote-run-operation-result', 1)
class PollRunOperationCompleteResult(
    RemoteRunOperationResult,
    PollResult,
):
    state: TaskHandlerState = field(
        metadata=restrict_to(TaskHandlerState.Success,
                             TaskHandlerState.Failed),
    )

    @classmethod
    def from_result(
        cls: Type['PollRunOperationCompleteResult'],
        base: RemoteRunOperationResult,
        tags: TaskTags,
        timing: TaskTiming,
        logs: List[LogMessage],
    ) -> 'PollRunOperationCompleteResult':
        return cls(
            success=base.success,
            results=base.results,
            generated_at=base.generated_at,
            elapsed_time=base.elapsed_time,
            logs=logs,
            tags=tags,
            state=timing.state,
            start=timing.start,
            end=timing.end,
            elapsed=timing.elapsed,
        )


@dataclass
@schema_version('poll-remote-catalog-result', 1)
class PollCatalogCompleteResult(RemoteCatalogResults, PollResult):
    state: TaskHandlerState = field(
        metadata=restrict_to(TaskHandlerState.Success,
                             TaskHandlerState.Failed),
    )

    @classmethod
    def from_result(
        cls: Type['PollCatalogCompleteResult'],
        base: RemoteCatalogResults,
        tags: TaskTags,
        timing: TaskTiming,
        logs: List[LogMessage],
    ) -> 'PollCatalogCompleteResult':
        return cls(
            nodes=base.nodes,
            sources=base.sources,
            generated_at=base.generated_at,
            errors=base.errors,
            _compile_results=base._compile_results,
            logs=logs,
            tags=tags,
            state=timing.state,
            start=timing.start,
            end=timing.end,
            elapsed=timing.elapsed,
        )


@dataclass
@schema_version('poll-remote-in-progress-result', 1)
class PollInProgressResult(PollResult):
    pass


@dataclass
@schema_version('poll-remote-get-manifest-result', 1)
class PollGetManifestResult(GetManifestResult, PollResult):
    state: TaskHandlerState = field(
        metadata=restrict_to(TaskHandlerState.Success,
                             TaskHandlerState.Failed),
    )

    @classmethod
    def from_result(
        cls: Type['PollGetManifestResult'],
        base: GetManifestResult,
        tags: TaskTags,
        timing: TaskTiming,
        logs: List[LogMessage],
    ) -> 'PollGetManifestResult':
        return cls(
            manifest=base.manifest,
            logs=logs,
            tags=tags,
            state=timing.state,
            start=timing.start,
            end=timing.end,
            elapsed=timing.elapsed,
        )


@dataclass
@schema_version('poll-remote-freshness-result', 1)
class PollFreshnessResult(RemoteFreshnessResult, PollResult):
    state: TaskHandlerState = field(
        metadata=restrict_to(TaskHandlerState.Success,
                             TaskHandlerState.Failed),
    )

    @classmethod
    def from_result(
        cls: Type['PollFreshnessResult'],
        base: RemoteFreshnessResult,
        tags: TaskTags,
        timing: TaskTiming,
        logs: List[LogMessage],
    ) -> 'PollFreshnessResult':
        return cls(
            logs=logs,
            tags=tags,
            state=timing.state,
            start=timing.start,
            end=timing.end,
            elapsed=timing.elapsed,
            metadata=base.metadata,
            results=base.results,
            elapsed_time=base.elapsed_time,
        )

# Manifest parsing types


class ManifestStatus(StrEnum):
    Init = 'init'
    Compiling = 'compiling'
    Ready = 'ready'
    Error = 'error'


@dataclass
@schema_version('remote-status-result', 1)
class LastParse(RemoteResult):
    state: ManifestStatus = ManifestStatus.Init
    logs: List[LogMessage] = field(default_factory=list)
    error: Optional[Dict[str, Any]] = None
    timestamp: datetime = field(default_factory=datetime.utcnow)
    pid: int = field(default_factory=os.getpid)
