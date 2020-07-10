import enum
import os
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Optional, Union, List, Any, Dict, Type

from hologram import JsonSchemaMixin
from hologram.helpers import StrEnum

from dbt.contracts.graph.compiled import CompileResultNode
from dbt.contracts.graph.manifest import WritableManifest
from dbt.contracts.results import (
    TimingInfo,
    CatalogResults,
    ExecutionResult,
)
from dbt.exceptions import InternalException
from dbt.logger import LogMessage
from dbt.utils import restrict_to


TaskTags = Optional[Dict[str, Any]]
TaskID = uuid.UUID

# Inputs


@dataclass
class RPCParameters(JsonSchemaMixin):
    timeout: Optional[float]
    task_tags: TaskTags


@dataclass
class RPCExecParameters(RPCParameters):
    name: str
    sql: str
    macros: Optional[str]


@dataclass
class RPCCompileParameters(RPCParameters):
    threads: Optional[int] = None
    models: Union[None, str, List[str]] = None
    exclude: Union[None, str, List[str]] = None
    selector: Optional[str] = None


@dataclass
class RPCSnapshotParameters(RPCParameters):
    threads: Optional[int] = None
    select: Union[None, str, List[str]] = None
    exclude: Union[None, str, List[str]] = None
    selector: Optional[str] = None


@dataclass
class RPCTestParameters(RPCCompileParameters):
    data: bool = False
    schema: bool = False


@dataclass
class RPCSeedParameters(RPCParameters):
    threads: Optional[int] = None
    select: Union[None, str, List[str]] = None
    exclude: Union[None, str, List[str]] = None
    selector: Optional[str] = None
    show: bool = False


@dataclass
class RPCDocsGenerateParameters(RPCParameters):
    compile: bool = True


@dataclass
class RPCCliParameters(RPCParameters):
    cli: str


@dataclass
class RPCNoParameters(RPCParameters):
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
class GCSettings(JsonSchemaMixin):
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


@dataclass
class GetManifestParameters(RPCParameters):
    pass

# Outputs


@dataclass
class RemoteResult(JsonSchemaMixin):
    logs: List[LogMessage]


@dataclass
class RemoteEmptyResult(RemoteResult):
    pass


@dataclass
class RemoteCatalogResults(CatalogResults, RemoteResult):
    pass


@dataclass
class RemoteCompileResult(RemoteResult):
    raw_sql: str
    compiled_sql: str
    node: CompileResultNode
    timing: List[TimingInfo]

    @property
    def error(self):
        return None


@dataclass
class RemoteExecutionResult(ExecutionResult, RemoteResult):
    pass


@dataclass
class ResultTable(JsonSchemaMixin):
    column_names: List[str]
    rows: List[Any]


@dataclass
class RemoteRunOperationResult(ExecutionResult, RemoteResult):
    success: bool


@dataclass
class RemoteRunResult(RemoteCompileResult):
    table: ResultTable


RPCResult = Union[
    RemoteCompileResult,
    RemoteExecutionResult,
    RemoteCatalogResults,
    RemoteEmptyResult,
    RemoteRunOperationResult,
]


# GC types


class GCResultState(StrEnum):
    Deleted = 'deleted'  # successful GC
    Missing = 'missing'  # nothing to GC
    Running = 'running'  # can't GC


@dataclass
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
class TaskTiming(JsonSchemaMixin):
    state: TaskHandlerState
    start: Optional[datetime]
    end: Optional[datetime]
    elapsed: Optional[float]


@dataclass
class TaskRow(TaskTiming):
    task_id: TaskID
    request_id: Union[str, int]
    request_source: str
    method: str
    timeout: Optional[float]
    tags: TaskTags


@dataclass
class PSResult(RemoteResult):
    rows: List[TaskRow]


class KillResultStatus(StrEnum):
    Missing = 'missing'
    NotStarted = 'not_started'
    Killed = 'killed'
    Finished = 'finished'


@dataclass
class KillResult(RemoteResult):
    state: KillResultStatus = KillResultStatus.Missing
    logs: List[LogMessage] = field(default_factory=list)


@dataclass
class GetManifestResult(RemoteResult):
    manifest: Optional[WritableManifest]


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


@dataclass
class PollRemoteEmptyCompleteResult(PollResult, RemoteEmptyResult):
    state: TaskHandlerState = field(
        metadata=restrict_to(TaskHandlerState.Success,
                             TaskHandlerState.Failed),
    )

    @classmethod
    def from_result(
        cls: Type['PollRemoteEmptyCompleteResult'],
        base: RemoteEmptyResult,
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
        )


@dataclass
class PollKilledResult(PollResult):
    state: TaskHandlerState = field(
        metadata=restrict_to(TaskHandlerState.Killed),
    )


@dataclass
class PollExecuteCompleteResult(RemoteExecutionResult, PollResult):
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
class PollCompileCompleteResult(RemoteCompileResult, PollResult):
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
        )


@dataclass
class PollRunCompleteResult(RemoteRunResult, PollResult):
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
        )


@dataclass
class PollRunOperationCompleteResult(RemoteRunOperationResult, PollResult):
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
class PollInProgressResult(PollResult):
    pass


@dataclass
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

# Manifest parsing types


class ManifestStatus(StrEnum):
    Init = 'init'
    Compiling = 'compiling'
    Ready = 'ready'
    Error = 'error'


@dataclass
class LastParse(RemoteResult):
    state: ManifestStatus = ManifestStatus.Init
    logs: List[LogMessage] = field(default_factory=list)
    error: Optional[Dict[str, Any]] = None
    timestamp: datetime = field(default_factory=datetime.utcnow)
    pid: int = field(default_factory=os.getpid)
