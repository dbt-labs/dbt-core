import multiprocessing
import os
import signal
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from functools import wraps
from typing import Any, Dict, Optional, List, Union, Set, Callable

from hologram import JsonSchemaMixin
from hologram.helpers import StrEnum

import dbt.exceptions
from dbt.contracts.results import (
    RemoteCompileResult,
    RemoteRunResult,
    RemoteExecutionResult,
)
from dbt.logger import LogMessage
from dbt.rpc.error import dbt_error, RPCException
from dbt.rpc.task_handler import TaskHandlerState, RequestTaskHandler
from dbt.rpc.task import RemoteCallable
from dbt.utils import restrict_to


@dataclass
class TaskRow(JsonSchemaMixin):
    task_id: uuid.UUID
    request_id: Union[str, int]
    request_source: str
    method: str
    state: TaskHandlerState
    start: Optional[datetime]
    elapsed: Optional[float]
    timeout: Optional[float]

    @classmethod
    def from_task(cls, task_handler: RequestTaskHandler, now_time: datetime):
        # get information about the task in a way that should not provide any
        # conflicting information. Calculate elapsed time based on `now_time`
        state = task_handler.state
        if state == TaskHandlerState.NotStarted:
            start = None
            elapsed = None
        else:
            if task_handler.started is None:
                raise dbt.exceptions.InternalException(
                    'task handler started but start time is not set'
                )
            start = task_handler.started
            elapsed = (now_time - start).total_seconds()

        return cls(
            task_id=task_handler.task_id,
            request_id=task_handler.request_id,
            request_source=task_handler.request_source,
            method=task_handler.method,
            state=state,
            start=start,
            elapsed=elapsed,
            timeout=task_handler.timeout,
        )


class KillResultStatus(StrEnum):
    Missing = 'missing'
    NotStarted = 'not_started'
    Killed = 'killed'
    Finished = 'finished'


@dataclass
class KillResult(JsonSchemaMixin):
    status: KillResultStatus


@dataclass
class PollResult(JsonSchemaMixin):
    status: TaskHandlerState


@dataclass
class PollExecuteSuccessResult(PollResult, RemoteExecutionResult):
    status: TaskHandlerState = field(
        metadata=restrict_to(TaskHandlerState.Success)
    )

    @classmethod
    def from_result(cls, status, base):
        return cls(
            status=status,
            results=base.results,
            generated_at=base.generated_at,
            elapsed_time=base.elapsed_time,
            logs=base.logs,
        )


@dataclass
class PollCompileSuccessResult(PollResult, RemoteCompileResult):
    status: TaskHandlerState = field(
        metadata=restrict_to(TaskHandlerState.Success)
    )

    @classmethod
    def from_result(cls, status, base):
        return cls(
            status=status,
            raw_sql=base.raw_sql,
            compiled_sql=base.compiled_sql,
            node=base.node,
            timing=base.timing,
            logs=base.logs,
        )


@dataclass
class PollRunSuccessResult(PollResult, RemoteRunResult):
    status: TaskHandlerState = field(
        metadata=restrict_to(TaskHandlerState.Success)
    )

    @classmethod
    def from_result(cls, status, base):
        return cls(
            status=status,
            raw_sql=base.raw_sql,
            compiled_sql=base.compiled_sql,
            node=base.node,
            timing=base.timing,
            logs=base.logs,
            table=base.table,
        )


def poll_success(status, logs, result):
    if status != TaskHandlerState.Success:
        raise dbt.exceptions.InternalException(
            'got invalid result status in poll_success: {}'.format(status)
        )

    if isinstance(result, RemoteExecutionResult):
        return PollExecuteSuccessResult.from_result(status=status, base=result)
    # order matters here, as RemoteRunResult subclasses RemoteCompileResult
    elif isinstance(result, RemoteRunResult):
        return PollRunSuccessResult.from_result(status=status, base=result)
    elif isinstance(result, RemoteCompileResult):
        return PollCompileSuccessResult.from_result(status=status, base=result)
    else:
        raise dbt.exceptions.InternalException(
            'got invalid result in poll_success: {}'.format(result)
        )


@dataclass
class PollInProgressResult(PollResult):
    logs: List[LogMessage]


@dataclass
class PSResult(JsonSchemaMixin):
    rows: List[TaskRow]


class ManifestStatus(StrEnum):
    Init = 'init'
    Compiling = 'compiling'
    Ready = 'ready'
    Error = 'error'


@dataclass
class LastCompile(JsonSchemaMixin):
    status: ManifestStatus
    error: Optional[Dict[str, Any]] = None
    logs: Optional[List[Dict[str, Any]]] = None
    timestamp: datetime = field(default_factory=datetime.utcnow)
    pid: int = field(default_factory=os.getpid)


UnmanagedHandler = Callable[..., JsonSchemaMixin]
WrappedHandler = Callable[..., Dict[str, Any]]


def _wrap_builtin(func: UnmanagedHandler) -> WrappedHandler:
    @wraps(func)
    def inner(*args, **kwargs):
        return func(*args, **kwargs).to_dict(omit_none=False)
    return inner


class TaskManager:
    def __init__(self, args, config):
        self.args = args
        self.config = config
        self.tasks: Dict[uuid.UUID, RequestTaskHandler] = {}
        self._rpc_task_map = {}
        self._builtins: Dict[str, UnmanagedHandler] = {}
        self.last_compile = LastCompile(status=ManifestStatus.Init)
        self._lock = multiprocessing.Lock()

    def add_request(self, request_handler):
        self.tasks[request_handler.task_id] = request_handler

    def reserve_handler(self, task):
        self._rpc_task_map[task.METHOD_NAME] = None

    def add_task_handler(self, task, manifest):
        self._rpc_task_map[task.METHOD_NAME] = task(
            self.args, self.config, manifest
        )

    def rpc_task(self, method_name):
        with self._lock:
            return self._rpc_task_map[method_name]

    def ready(self):
        with self._lock:
            return self.last_compile.status == ManifestStatus.Ready

    def set_compiling(self):
        assert self.last_compile.status != ManifestStatus.Compiling, \
            f'invalid state {self.last_compile.status}'
        with self._lock:
            self.last_compile = LastCompile(status=ManifestStatus.Compiling)

    def set_compile_exception(self, exc, logs=List[Dict[str, Any]]):
        assert self.last_compile.status == ManifestStatus.Compiling, \
            f'invalid state {self.last_compile.status}'
        self.last_compile = LastCompile(
            error={'message': str(exc)},
            status=ManifestStatus.Error,
            logs=logs
        )

    def set_ready(self, logs=List[Dict[str, Any]]) -> None:
        assert self.last_compile.status == ManifestStatus.Compiling, \
            f'invalid state {self.last_compile.status}'
        self.last_compile = LastCompile(
            status=ManifestStatus.Ready,
            logs=logs
        )

    def process_status(self) -> LastCompile:
        with self._lock:
            last_compile = self.last_compile
        return last_compile

    def process_ps(
        self,
        active: bool = True,
        completed: bool = False,
    ) -> PSResult:
        rows = []
        now = datetime.utcnow()
        with self._lock:
            for task in self.tasks.values():
                row = TaskRow.from_task(task, now)
                if row.state.finished and completed:
                    rows.append(row)
                elif not row.state.finished and active:
                    rows.append(row)

        rows.sort(key=lambda r: (r.state, r.start, r.method))
        result = PSResult(rows=rows)
        return result

    def process_kill(self, task_id: str) -> KillResult:
        task_id_uuid = uuid.UUID(task_id)

        status = KillResultStatus.Missing
        try:
            task = self.tasks[task_id_uuid]
        except KeyError:
            # nothing to do!
            return KillResult(status)

        status = KillResultStatus.NotStarted

        if task.process is None:
            return KillResult(status)
        pid = task.process.pid
        if pid is None:
            return KillResult(status)

        if task.process.is_alive():
            os.kill(pid, signal.SIGINT)
            status = KillResultStatus.Killed
        else:
            status = KillResultStatus.Finished

        return KillResult(status)

    def process_poll(
        self,
        request_token: str,
        logs: bool = False,
        logs_start: int = 0,
    ) -> PollResult:
        task_id = uuid.UUID(request_token)
        try:
            task = self.tasks[task_id]
        except KeyError:
            # We don't recognize that ID.
            raise dbt.exceptions.UnknownAsyncIDException(task_id) from None

        task_logs: List[LogMessage] = []
        if logs:
            task_logs = task.logs[logs_start:]

        # Get a state and store it locally so we ignore updates to state,
        # otherwise things will get confusing. States should always be
        # "forward-compatible" so if the state has transitioned to error/result
        # but we aren't there yet, the logs will still be valid.
        state = task.state
        if state == TaskHandlerState.Error:
            err = task.error
            if err is None:
                exc = dbt.exceptions.InternalException(
                    'At end of task {}, state={} but error is None'
                    .format(state, task_id)
                )
                raise RPCException.from_error(
                    dbt_error(exc, logs=[l.to_dict() for l in task_logs])
                )
            # the exception has logs already attached from the child, don't
            # overwrite those
            raise err
        elif state == TaskHandlerState.Success:
            if task.result is None:
                exc = dbt.exceptions.InternalException(
                    'At end of task {}, state={} but result is None'
                    .format(state, task_id)
                )
                raise RPCException.from_error(
                    dbt_error(exc, logs=[l.to_dict() for l in task_logs])
                )

            return poll_success(
                status=state,
                logs=task_logs,
                result=task.result,
            )

        return PollInProgressResult(state, task_logs)

    def _rpc_builtins(self) -> Dict[str, UnmanagedHandler]:
        if self._builtins:
            return self._builtins

        with self._lock:
            if self._builtins:  # handle a race
                return self._builtins

            methods: Dict[str, UnmanagedHandler] = {
                'ps': self.process_ps,
                'status': self.process_status,
                'poll': self.process_poll,
            }
            if os.name != 'nt':
                methods['kill'] = self.process_kill

            self._builtins.update(methods)
            return self._builtins

    def methods(self, builtin=True) -> Set[str]:
        all_methods: Set[str] = set()
        if builtin:
            all_methods.update(self._rpc_builtins())

        with self._lock:
            all_methods.update(self._rpc_task_map)

        return all_methods

    def currently_compiling(self, *args, **kwargs):
        """Raise an RPC exception to trigger the error handler."""
        raise dbt_error(dbt.exceptions.RPCCompiling('compile in progress'))

    def compilation_error(self, *args, **kwargs):
        """Raise an RPC exception to trigger the error handler."""
        raise dbt_error(
            dbt.exceptions.RPCLoadException(self.last_compile.error)
        )

    def get_handler(
        self, method, http_request, json_rpc_request
    ) -> Optional[Union[WrappedHandler, RemoteCallable]]:
        # the dispatcher's keys are method names and its values are functions
        # that implement the RPC calls
        _builtins = self._rpc_builtins()
        if method in _builtins:
            return _wrap_builtin(_builtins[method])
        elif method not in self._rpc_task_map:
            return None
        # if we have no manifest we want to return an error about why
        elif self.last_compile.status == ManifestStatus.Compiling:
            return self.currently_compiling
        elif self.last_compile.status == ManifestStatus.Error:
            return self.compilation_error
        else:
            return self.rpc_task(method)
