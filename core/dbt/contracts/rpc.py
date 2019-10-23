import enum
import os
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from numbers import Real
from typing import Optional, Union, List, Any, Dict, Type

from hologram import JsonSchemaMixin
from hologram.helpers import StrEnum

from dbt.contracts.graph.compiled import CompileResultNode
from dbt.contracts.results import (
    TimingInfo,
    CatalogResults,
    ExecutionResult,
)
from dbt.exceptions import InternalException
from dbt.logger import LogMessage
from dbt.utils import restrict_to


TaskTags = Optional[Dict[str, Any]]

# Inputs


@dataclass
class RPCParameters(JsonSchemaMixin):
    timeout: Optional[Real]
    task_tags: TaskTags


@dataclass
class RPCExecParameters(RPCParameters):
    name: str
    sql: str
    macros: Optional[str]


@dataclass
class RPCCompileParameters(RPCParameters):
    models: Union[None, str, List[str]] = None
    exclude: Union[None, str, List[str]] = None


@dataclass
class RPCTestParameters(RPCCompileParameters):
    data: bool = False
    schema: bool = False


@dataclass
class RPCSeedParameters(RPCParameters):
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
class RemoteRunResult(RemoteCompileResult):
    table: ResultTable


RPCResult = Union[
    RemoteCompileResult,
    RemoteExecutionResult,
    RemoteCatalogResults,
    RemoteEmptyResult,
]


# GC types


class GCResultState(StrEnum):
    Deleted = 'deleted'  # successful GC
    Missing = 'missing'  # nothing to GC
    Running = 'running'  # can't GC


@dataclass
class GCResultSet(JsonSchemaMixin):
    deleted: List[uuid.UUID] = field(default_factory=list)
    missing: List[uuid.UUID] = field(default_factory=list)
    running: List[uuid.UUID] = field(default_factory=list)

    def add_result(self, task_id: uuid.UUID, status: GCResultState):
        if status == GCResultState.Missing:
            self.missing.append(task_id)
        elif status == GCResultState.Running:
            self.running.append(task_id)
        elif status == GCResultState.Deleted:
            self.deleted.append(task_id)
        else:
            raise InternalException(
                f'Got invalid status in add_result: {status}'
            )


@dataclass
class GCSettings(JsonSchemaMixin):
    # start evicting the longest-ago-ended tasks here
    maxsize: int
    # start evicting all tasks before now - auto_reap_age when we have this
    # many tasks in the table
    reapsize: int
    # a positive timedelta indicating how far back we should go
    auto_reap_age: timedelta


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
class TaskRow(JsonSchemaMixin):
    task_id: uuid.UUID
    request_id: Union[str, int]
    request_source: str
    method: str
    state: TaskHandlerState
    start: Optional[datetime]
    end: Optional[datetime]
    elapsed: Optional[float]
    timeout: Optional[float]
    tags: TaskTags


@dataclass
class PSResult(JsonSchemaMixin):
    rows: List[TaskRow]


class KillResultStatus(StrEnum):
    Missing = 'missing'
    NotStarted = 'not_started'
    Killed = 'killed'
    Finished = 'finished'


@dataclass
class KillResult(JsonSchemaMixin):
    status: KillResultStatus


# this is kind of carefuly structured: BlocksManifestTasks is implied by
# RequiresConfigReloadBefore and RequiresManifestReloadAfter
class RemoteMethodFlags(enum.Flag):
    Empty = 0
    BlocksManifestTasks = 1
    RequiresConfigReloadBefore = 3
    RequiresManifestReloadAfter = 5


# Polling types


@dataclass
class PollResult(JsonSchemaMixin):
    tags: TaskTags = None
    status: TaskHandlerState = TaskHandlerState.NotStarted


@dataclass
class PollRemoteEmptyCompleteResult(PollResult, RemoteEmptyResult):
    status: TaskHandlerState = field(
        metadata=restrict_to(TaskHandlerState.Success,
                             TaskHandlerState.Failed),
        default=TaskHandlerState.Success,
    )

    @classmethod
    def from_result(
        cls: Type['PollRemoteEmptyCompleteResult'],
        status: TaskHandlerState,
        base: RemoteEmptyResult,
        tags: TaskTags,
    ) -> 'PollRemoteEmptyCompleteResult':
        return cls(
            status=status,
            logs=base.logs,
            tags=tags,
        )


@dataclass
class PollKilledResult(PollResult):
    logs: List[LogMessage] = field(default_factory=list)
    status: TaskHandlerState = field(
        metadata=restrict_to(TaskHandlerState.Killed),
        default=TaskHandlerState.Killed,
    )


@dataclass
class PollExecuteCompleteResult(PollResult, RemoteExecutionResult):
    status: TaskHandlerState = field(
        metadata=restrict_to(TaskHandlerState.Success,
                             TaskHandlerState.Failed),
        default=TaskHandlerState.Success,
    )

    @classmethod
    def from_result(
        cls: Type['PollExecuteCompleteResult'],
        status: TaskHandlerState,
        base: RemoteExecutionResult,
        tags: TaskTags,
    ) -> 'PollExecuteCompleteResult':
        return cls(
            status=status,
            results=base.results,
            generated_at=base.generated_at,
            elapsed_time=base.elapsed_time,
            logs=base.logs,
            tags=tags,
        )


@dataclass
class PollCompileCompleteResult(PollResult, RemoteCompileResult):
    status: TaskHandlerState = field(
        metadata=restrict_to(TaskHandlerState.Success,
                             TaskHandlerState.Failed),
        default=TaskHandlerState.Success,
    )

    @classmethod
    def from_result(
        cls: Type['PollCompileCompleteResult'],
        status: TaskHandlerState,
        base: RemoteCompileResult,
        tags: TaskTags,
    ) -> 'PollCompileCompleteResult':
        return cls(
            status=status,
            raw_sql=base.raw_sql,
            compiled_sql=base.compiled_sql,
            node=base.node,
            timing=base.timing,
            logs=base.logs,
            tags=tags,
        )


@dataclass
class PollRunCompleteResult(PollResult, RemoteRunResult):
    status: TaskHandlerState = field(
        metadata=restrict_to(TaskHandlerState.Success,
                             TaskHandlerState.Failed),
        default=TaskHandlerState.Success,
    )

    @classmethod
    def from_result(
        cls: Type['PollRunCompleteResult'],
        status: TaskHandlerState,
        base: RemoteRunResult,
        tags: TaskTags,
    ) -> 'PollRunCompleteResult':
        return cls(
            status=status,
            raw_sql=base.raw_sql,
            compiled_sql=base.compiled_sql,
            node=base.node,
            timing=base.timing,
            logs=base.logs,
            table=base.table,
            tags=tags,
        )


@dataclass
class PollCatalogCompleteResult(PollResult, RemoteCatalogResults):
    status: TaskHandlerState = field(
        metadata=restrict_to(TaskHandlerState.Success,
                             TaskHandlerState.Failed),
        default=TaskHandlerState.Success,
    )

    @classmethod
    def from_result(
        cls: Type['PollCatalogCompleteResult'],
        status: TaskHandlerState,
        base: RemoteCatalogResults,
        tags: TaskTags,
    ) -> 'PollCatalogCompleteResult':
        return cls(
            status=status,
            nodes=base.nodes,
            generated_at=base.generated_at,
            _compile_results=base._compile_results,
            logs=base.logs,
            tags=tags,
        )


@dataclass
class PollInProgressResult(PollResult):
    logs: List[LogMessage] = field(default_factory=list)


# Manifest parsing types

class ManifestStatus(StrEnum):
    Init = 'init'
    Compiling = 'compiling'
    Ready = 'ready'
    Error = 'error'


@dataclass
class LastParse(JsonSchemaMixin):
    status: ManifestStatus
    error: Optional[Dict[str, Any]] = None
    logs: Optional[List[Dict[str, Any]]] = None
    timestamp: datetime = field(default_factory=datetime.utcnow)
    pid: int = field(default_factory=os.getpid)
