import operator
import os
import signal
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from functools import wraps
from typing import (
    Any, Dict, Optional, List, Union, Set, Callable, Iterable, Tuple, Type,
)

from hologram import JsonSchemaMixin, ValidationError
from hologram.helpers import StrEnum

import dbt.exceptions
import dbt.flags
from dbt.contracts.graph.manifest import Manifest
from dbt.contracts.rpc import (
    RemoteCompileResult,
    RemoteRunResult,
    RemoteExecutionResult,
    RemoteCatalogResults,
    RemoteEmptyResult,
)
from dbt.logger import LogMessage
from dbt.rpc.error import dbt_error, RPCException
from dbt.rpc.task_handler import TaskHandlerState, RequestTaskHandler
from dbt.rpc.method import RemoteMethod, RemoteManifestMethod

from dbt.utils import restrict_to

# import this to make sure our timedelta encoder is registered
from dbt import helper_types  # noqa


def _assert_started(task_handler: RequestTaskHandler) -> datetime:
    if task_handler.started is None:
        raise dbt.exceptions.InternalException(
            'task handler started but start time is not set'
        )
    return task_handler.started


def _assert_ended(task_handler: RequestTaskHandler) -> datetime:
    if task_handler.ended is None:
        raise dbt.exceptions.InternalException(
            'task handler finished but end time is not set'
        )
    return task_handler.ended


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
    tags: Optional[Dict[str, Any]]

    @classmethod
    def from_task(cls, task_handler: RequestTaskHandler, now_time: datetime):
        # get information about the task in a way that should not provide any
        # conflicting information. Calculate elapsed time based on `now_time`
        state = task_handler.state
        # store end/start so 'ps' output always makes sense:
        # not started -> no start time/elapsed, running -> no end time, etc
        end = None
        start = None
        elapsed = None
        if state > TaskHandlerState.NotStarted:
            start = _assert_started(task_handler)
            elapsed_end = now_time

            if state.finished:
                elapsed_end = _assert_ended(task_handler)
                end = elapsed_end

            elapsed = (elapsed_end - start).total_seconds()

        return cls(
            task_id=task_handler.task_id,
            request_id=task_handler.request_id,
            request_source=task_handler.request_source,
            method=task_handler.method,
            state=state,
            start=start,
            end=end,
            elapsed=elapsed,
            timeout=task_handler.timeout,
            tags=task_handler.tags,
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
    tags: Optional[Dict[str, Any]] = None
    status: TaskHandlerState = TaskHandlerState.NotStarted


class GCResultState(StrEnum):
    Deleted = 'deleted'  # successful GC
    Missing = 'missing'  # nothing to GC
    Running = 'running'  # can't GC


@dataclass
class _GCResult(JsonSchemaMixin):
    task_id: uuid.UUID
    status: GCResultState


@dataclass
class GCResultSet(JsonSchemaMixin):
    deleted: List[uuid.UUID] = field(default_factory=list)
    missing: List[uuid.UUID] = field(default_factory=list)
    running: List[uuid.UUID] = field(default_factory=list)

    def add_result(self, result: _GCResult):
        if result.status == GCResultState.Missing:
            self.missing.append(result.task_id)
        elif result.status == GCResultState.Running:
            self.running.append(result.task_id)
        elif result.status == GCResultState.Deleted:
            self.deleted.append(result.task_id)
        else:
            raise dbt.exceptions.InternalException(
                'Got invalid _GCResult in add_result: {!r}'
                .format(result)
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


@dataclass
class _GCArguments(JsonSchemaMixin):
    task_ids: Optional[List[uuid.UUID]]
    before: Optional[datetime]
    settings: Optional[GCSettings]


TaskTags = Optional[Dict[str, Any]]


@dataclass
class PollRemoteEmptySuccessResult(PollResult, RemoteEmptyResult):
    status: TaskHandlerState = field(
        metadata=restrict_to(TaskHandlerState.Success),
        default=TaskHandlerState.Success,
    )

    @classmethod
    def from_result(
        cls: Type['PollRemoteEmptySuccessResult'],
        status: TaskHandlerState,
        base: RemoteEmptyResult,
        tags: TaskTags,
    ) -> 'PollRemoteEmptySuccessResult':
        return cls(
            status=status,
            logs=base.logs,
            tags=tags,
        )


@dataclass
class PollExecuteSuccessResult(PollResult, RemoteExecutionResult):
    status: TaskHandlerState = field(
        metadata=restrict_to(TaskHandlerState.Success),
        default=TaskHandlerState.Success,
    )

    @classmethod
    def from_result(
        cls: Type['PollExecuteSuccessResult'],
        status: TaskHandlerState,
        base: RemoteExecutionResult,
        tags: TaskTags,
    ) -> 'PollExecuteSuccessResult':
        return cls(
            status=status,
            results=base.results,
            generated_at=base.generated_at,
            elapsed_time=base.elapsed_time,
            logs=base.logs,
            tags=tags,
        )


@dataclass
class PollCompileSuccessResult(PollResult, RemoteCompileResult):
    status: TaskHandlerState = field(
        metadata=restrict_to(TaskHandlerState.Success),
        default=TaskHandlerState.Success,
    )

    @classmethod
    def from_result(
        cls: Type['PollCompileSuccessResult'],
        status: TaskHandlerState,
        base: RemoteCompileResult,
        tags: TaskTags,
    ) -> 'PollCompileSuccessResult':
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
class PollRunSuccessResult(PollResult, RemoteRunResult):
    status: TaskHandlerState = field(
        metadata=restrict_to(TaskHandlerState.Success),
        default=TaskHandlerState.Success,
    )

    @classmethod
    def from_result(
        cls: Type['PollRunSuccessResult'],
        status: TaskHandlerState,
        base: RemoteRunResult,
        tags: TaskTags,
    ) -> 'PollRunSuccessResult':
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
class PollCatalogSuccessResult(PollResult, RemoteCatalogResults):
    status: TaskHandlerState = field(
        metadata=restrict_to(TaskHandlerState.Success),
        default=TaskHandlerState.Success,
    )

    @classmethod
    def from_result(
        cls: Type['PollCatalogSuccessResult'],
        status: TaskHandlerState,
        base: RemoteCatalogResults,
        tags: TaskTags,
    ) -> 'PollCatalogSuccessResult':
        return cls(
            status=status,
            nodes=base.nodes,
            generated_at=base.generated_at,
            _compile_results=base._compile_results,
            logs=base.logs,
            tags=tags,
        )


def poll_success(
    status: TaskHandlerState, result: Any, tags: TaskTags
) -> PollResult:
    if status != TaskHandlerState.Success:
        raise dbt.exceptions.InternalException(
            'got invalid result status in poll_success: {}'.format(status)
        )

    if isinstance(result, RemoteExecutionResult):
        return PollExecuteSuccessResult.from_result(status, result, tags)
    # order matters here, as RemoteRunResult subclasses RemoteCompileResult
    elif isinstance(result, RemoteRunResult):
        return PollRunSuccessResult.from_result(status, result, tags)
    elif isinstance(result, RemoteCompileResult):
        return PollCompileSuccessResult.from_result(status, result, tags)
    elif isinstance(result, RemoteCatalogResults):
        return PollCatalogSuccessResult.from_result(status, result, tags)
    elif isinstance(result, RemoteEmptyResult):
        return PollRemoteEmptySuccessResult.from_result(status, result, tags)
    else:
        raise dbt.exceptions.InternalException(
            'got invalid result in poll_success: {}'.format(result)
        )


@dataclass
class PollInProgressResult(PollResult):
    logs: List[LogMessage] = field(default_factory=list)


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


class Reserved:
    # a dummy class
    pass


class TaskManager:
    def __init__(self, args, config):
        self.args = args
        self.config = config
        self.tasks: Dict[uuid.UUID, RequestTaskHandler] = {}
        self._rpc_task_map: Dict[str, Union[Reserved, RemoteMethod]] = {}
        self._builtins: Dict[str, UnmanagedHandler] = {}
        self.last_compile = LastCompile(status=ManifestStatus.Init)
        self._lock: dbt.flags.MP_CONTEXT.Lock = dbt.flags.MP_CONTEXT.Lock()
        self._gc_settings = GCSettings(
            maxsize=1000, reapsize=500, auto_reap_age=timedelta(days=30)
        )

    def add_request(self, request_handler):
        self.tasks[request_handler.task_id] = request_handler

    def reserve_handler(self, task):
        self._rpc_task_map[task.METHOD_NAME] = Reserved()

    def _check_task_handler(self, task_type: Type[RemoteMethod]) -> None:
        if task_type.METHOD_NAME is None:
            raise dbt.exceptions.InternalException(
                'Task {} has no method name, cannot add it'.format(task_type)
            )
        method = task_type.METHOD_NAME
        if method not in self._rpc_task_map:
            # this is weird, but hey whatever
            return
        other_task = self._rpc_task_map[method]
        if isinstance(other_task, Reserved) or type(other_task) is task_type:
            return
        raise dbt.exceptions.InternalException(
            'Got two tasks with the same method name! {0} and {1} both '
            'have a method name of {0.METHOD_NAME}, but RPC method names '
            'should be unique'.format(task_type, other_task)
        )

    def add_manifest_task_handler(
        self, task: Type[RemoteManifestMethod], manifest: Manifest
    ) -> None:
        self._check_task_handler(task)
        assert task.METHOD_NAME is not None
        self._rpc_task_map[task.METHOD_NAME] = task(
            self.args, self.config, manifest
        )

    def add_basic_task_handler(self, task: Type[RemoteMethod]) -> None:
        if issubclass(task, RemoteManifestMethod):
            raise dbt.exceptions.InternalException(
                f'Task {task} requires a manifest, cannot add it as a basic '
                f'handler'
            )

        self._check_task_handler(task)
        assert task.METHOD_NAME is not None
        self._rpc_task_map[task.METHOD_NAME] = task(self.args, self.config)

    def rpc_task(self, method_name: str) -> Union[Reserved, RemoteMethod]:
        with self._lock:
            return self._rpc_task_map[method_name]

    def ready(self) -> bool:
        with self._lock:
            return self.last_compile.status == ManifestStatus.Ready

    def set_compiling(self) -> None:
        assert self.last_compile.status != ManifestStatus.Compiling, \
            f'invalid state {self.last_compile.status}'
        with self._lock:
            self.last_compile = LastCompile(status=ManifestStatus.Compiling)

    def set_compile_exception(self, exc, logs=List[Dict[str, Any]]) -> None:
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
            task: RequestTaskHandler = self.tasks[task_id]
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
                result=task.result,
                tags=task.tags,
            )

        return PollInProgressResult(
            status=state,
            tags=task.tags,
            logs=task_logs,
        )

    def _rpc_builtins(self) -> Dict[str, UnmanagedHandler]:
        if self._builtins:
            return self._builtins

        with self._lock:
            if self._builtins:  # handle a race
                return self._builtins

            methods: Dict[str, UnmanagedHandler] = {
                'kill': self.process_kill,
                'ps': self.process_ps,
                'status': self.process_status,
                'poll': self.process_poll,
                'gc': self.process_gc,
            }

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

    def internal_error_for(self, msg) -> WrappedHandler:
        def _error(*args, **kwargs):
            raise dbt.exceptions.InternalException(msg)
        return _wrap_builtin(_error)

    def get_handler(
        self, method, http_request, json_rpc_request
    ) -> Optional[Union[WrappedHandler, RemoteMethod]]:
        # get_handler triggers a GC check. TODO: does this go somewhere else?
        self.gc_as_required()
        # the dispatcher's keys are method names and its values are functions
        # that implement the RPC calls
        _builtins = self._rpc_builtins()
        if method in _builtins:
            return _wrap_builtin(_builtins[method])
        elif method not in self._rpc_task_map:
            return None

        task = self.rpc_task(method)
        # If the task we got back was reserved, it must be a task that requires
        # a manifest and we don't have one. So we had better have a state of
        # compiling or error.

        if isinstance(task, Reserved):
            status = self.last_compile.status
            if status == ManifestStatus.Compiling:
                return self.currently_compiling
            elif status == ManifestStatus.Error:
                return self.compilation_error
            else:
                # if we got here, there is an error in dbt :(
                return self.internal_error_for(
                    f'Got a None task for {method}, state is {status}'
                )
        else:
            return task

    def _remove_task_if_finished(self, task_id: uuid.UUID) -> GCResultState:
        """Remove the task if it was finished. Raises a KeyError if the entry
        is removed during operation (so hold the lock).
        """
        if task_id not in self.tasks:
            return GCResultState.Missing

        task = self.tasks[task_id]
        if not task.state.finished:
            return GCResultState.Running

        del self.tasks[task_id]
        return GCResultState.Deleted

    def _gc_task_id(self, task_id: uuid.UUID) -> _GCResult:
        """To 'gc' a task ID, we just delete it from the tasks dict.

        You must hold the lock, as this mutates `tasks`.
        """
        try:
            status = self._remove_task_if_finished(task_id)
        except KeyError:
            # someone was mutating tasks while we had the lock, that's
            # not right!
            raise dbt.exceptions.InternalException(
                'Got a KeyError for task uuid={} during gc'
                .format(task_id)
            )

        return _GCResult(task_id=task_id, status=status)

    def _get_gc_before_list(self, when: datetime) -> List[uuid.UUID]:
        removals: List[uuid.UUID] = []
        for task in self.tasks.values():
            if not task.state.finished:
                continue
            elif task.ended is None:
                continue
            elif task.ended < when:
                removals.append(task.task_id)

        return removals

    def _get_oldest_ended_list(self, num: int) -> List[uuid.UUID]:
        candidates: List[Tuple[datetime, uuid.UUID]] = []
        for task in self.tasks.values():
            if not task.state.finished:
                continue
            elif task.ended is None:
                continue
            else:
                candidates.append((task.ended, task.task_id))
        candidates.sort(key=operator.itemgetter(0))
        return [task_id for _, task_id in candidates[:num]]

    def _gc_multiple_task_ids(
        self, task_ids: Iterable[uuid.UUID]
    ) -> GCResultSet:
        result = GCResultSet()
        for task_id in task_ids:
            gc_result = self._gc_task_id(task_id)
            result.add_result(gc_result)
        return result

    def gc_safe(
        self,
        task_ids: Optional[List[uuid.UUID]] = None,
        before: Optional[datetime] = None,
    ) -> GCResultSet:
        to_gc = set()

        if task_ids is not None:
            to_gc.update(task_ids)

        with self._lock:
            # we need the lock for this!
            if before is not None:
                to_gc.update(self._get_gc_before_list(before))
            return self._gc_multiple_task_ids(to_gc)

    def _gc_as_required_unsafe(self) -> None:
        to_remove: List[uuid.UUID] = []
        num_tasks = len(self.tasks)
        if num_tasks > self._gc_settings.maxsize:
            num = self._gc_settings.maxsize - num_tasks
            to_remove = self._get_oldest_ended_list(num)
        elif num_tasks > self._gc_settings.reapsize:
            before = datetime.utcnow() - self._gc_settings.auto_reap_age
            to_remove = self._get_gc_before_list(before)

        if to_remove:
            self._gc_multiple_task_ids(to_remove)

    def gc_as_required(self) -> None:
        with self._lock:
            return self._gc_as_required_unsafe()

    def process_gc(
        self,
        task_ids: Optional[List[str]] = None,
        before: Optional[str] = None,
        settings: Optional[Dict[str, Any]] = None,
    ) -> GCResultSet:
        """The gc endpoint takes three arguments, any of which may be present:

        - task_ids: An optional list of task ID UUIDs to try to GC
        - before: If provided, should be a datetime string. All tasks that
            finished before that datetime will be GCed
        - settings: If provided, should be a GCSettings object in JSON form.
            It will be applied to the task manager before GC starts. By default
            the existing gc settings remain.
        """
        try:
            args = _GCArguments.from_dict({
                'task_ids': task_ids,
                'before': before,
                'settings': settings,
            })
        except ValidationError as exc:
            # trigger the jsonrpc library to recognize the arguments as bad
            raise TypeError('bad arguments: {}'.format(exc))

        if args.settings:
            self._gc_settings = args.settings

        return self.gc_safe(task_ids=args.task_ids, before=args.before)
