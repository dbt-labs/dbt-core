import operator
import os
import signal
import threading
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta
from functools import wraps
from typing import (
    Any, Dict, Optional, List, Union, Set, Callable, Iterable, Tuple, Type,
)

from hologram import JsonSchemaMixin, ValidationError

import dbt.exceptions
import dbt.flags
from dbt.contracts.graph.manifest import Manifest
from dbt.contracts.rpc import (
    TaskTags,
    LastParse,
    ManifestStatus,
    GCSettings,
    KillResult,
    KillResultStatus,
    GCResultState,
    GCResultSet,
    TaskRow,
    PSResult,
    RemoteExecutionResult,
    RemoteRunResult,
    RemoteCompileResult,
    RemoteCatalogResults,
    RemoteEmptyResult,
    PollResult,
    PollInProgressResult,
    PollKilledResult,
    PollExecuteCompleteResult,
    PollRunCompleteResult,
    PollCompileCompleteResult,
    PollCatalogCompleteResult,
    PollRemoteEmptyCompleteResult,
)
from dbt.logger import LogMessage, list_handler
from dbt.perf_utils import get_full_manifest
from dbt.rpc.error import dbt_error, RPCException
from dbt.rpc.task_handler import (
    TaskHandlerState, RequestTaskHandler, set_parse_state_with
)
from dbt.rpc.method import RemoteMethod, RemoteManifestMethod, TaskList


# import this to make sure our timedelta encoder is registered
from dbt import helper_types  # noqa
from dbt.utils import env_set_truthy


SINGLE_THREADED_WEBSERVER = env_set_truthy('DBT_SINGLE_THREADED_WEBSERVER')


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


def make_task(task_handler: RequestTaskHandler, now_time: datetime) -> TaskRow:
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

    return TaskRow(
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


class ManifestReloader(threading.Thread):
    def __init__(self, task_manager: 'TaskManager') -> None:
        super().__init__()
        self.task_manager = task_manager

    def run(self) -> None:
        logs: List[LogMessage] = []
        with set_parse_state_with(self.task_manager, lambda: logs):
            with list_handler(logs):
                self.task_manager.parse_manifest()


@dataclass
class _GCArguments(JsonSchemaMixin):
    """An argument validation helper"""
    task_ids: Optional[List[uuid.UUID]]
    before: Optional[datetime]
    settings: Optional[GCSettings]


def poll_complete(
    status: TaskHandlerState, result: Any, tags: TaskTags
) -> PollResult:
    if status not in (TaskHandlerState.Success, TaskHandlerState.Failed):
        raise dbt.exceptions.InternalException(
            'got invalid result status in poll_complete: {}'.format(status)
        )

    cls: Type[Union[
        PollExecuteCompleteResult,
        PollRunCompleteResult,
        PollCompileCompleteResult,
        PollCatalogCompleteResult,
        PollRemoteEmptyCompleteResult,
    ]]

    if isinstance(result, RemoteExecutionResult):
        cls = PollExecuteCompleteResult
    # order matters here, as RemoteRunResult subclasses RemoteCompileResult
    elif isinstance(result, RemoteRunResult):
        cls = PollRunCompleteResult
    elif isinstance(result, RemoteCompileResult):
        cls = PollCompileCompleteResult
    elif isinstance(result, RemoteCatalogResults):
        cls = PollCatalogCompleteResult
    elif isinstance(result, RemoteEmptyResult):
        cls = PollRemoteEmptyCompleteResult
    else:
        raise dbt.exceptions.InternalException(
            'got invalid result in poll_complete: {}'.format(result)
        )
    return cls.from_result(status, result, tags)


class TaskManager:
    def __init__(self, args, config, task_types: TaskList) -> None:
        self.args = args
        self.config = config
        self._task_types: TaskList = task_types
        self.active_tasks: Dict[uuid.UUID, RequestTaskHandler] = {}
        self._rpc_task_map: Dict[str, Union[Reserved, RemoteMethod]] = {}
        self._builtins: Dict[str, UnmanagedHandler] = {}
        self.last_parse: LastParse = LastParse(status=ManifestStatus.Init)
        self._lock: dbt.flags.MP_CONTEXT.Lock = dbt.flags.MP_CONTEXT.Lock()
        self._gc_settings: GCSettings = GCSettings(
            maxsize=1000, reapsize=500, auto_reap_age=timedelta(days=30)
        )
        self._reloader: Optional[threading.Thread] = None

    def _reload_task_manager_thread(self, reloader: threading.Thread):
        """This function can only be running once at a time, as it runs in the
        signal handler we replace
        """
        # compile in a thread that will fix up the tag manager when it's done
        reloader.start()
        # only assign to _reloader here, to avoid calling join() before start()
        self._reloader = reloader

    def _reload_task_manager_fg(self, reloader: threading.Thread):
        """Override for single-threaded mode to run in the foreground"""
        # just reload directly
        reloader.run()

    def reload_manifest_tasks(self) -> bool:
        """Reload the manifest using a manifest reloader. Returns False if the
        reload was not started because it was already running.
        """
        if not self.set_parsing():
            return False
        if self._reloader is not None:
            # join() the existing reloader
            self._reloader.join()
        # perform the reload
        reloader = ManifestReloader(self)
        if self.single_threaded():
            self._reload_task_manager_fg(reloader)
        else:
            self._reload_task_manager_thread(reloader)
        return True

    def single_threaded(self):
        return SINGLE_THREADED_WEBSERVER or self.args.single_threaded

    def reload_non_manifest_tasks(self):
        # reload all the non-manifest tasks because the config changed.
        # manifest tasks are still blocked so we can ignore them
        for task_cls in self._task_types.non_manifest():
            self.add_basic_task_handler(task_cls)

    def reload_config(self):
        config = self.config.from_args(self.args)
        self.config = config
        # reload all the non-manifest tasks because the config changed.
        # manifest tasks are still blocked so we can ignore them
        self.reload_non_manifest_tasks()
        return config

    def add_request(self, request_handler: RequestTaskHandler):
        self.active_tasks[request_handler.task_id] = request_handler

    def reserve_handler(self, task: Type[RemoteMethod]) -> None:
        """Reserved tasks will return a status indicating that the manifest is
        compiling.
        """
        if task.METHOD_NAME is None:
            raise dbt.exceptions.InternalException(
                f'Cannot add task {task} as it has no method name'
            )
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
            return self.last_parse.status == ManifestStatus.Ready

    def set_parsing(self) -> bool:
        with self._lock:
            if self.last_parse.status == ManifestStatus.Compiling:
                return False
            self.last_parse = LastParse(status=ManifestStatus.Compiling)
        for task in self._task_types.manifest():
            # reserve any tasks that are invalid
            self.reserve_handler(task)
        return True

    def parse_manifest(self) -> None:
        manifest = get_full_manifest(self.config)

        for task_cls in self._task_types.manifest():
            self.add_manifest_task_handler(
                task_cls, manifest
            )

    def set_compile_exception(self, exc, logs=List[Dict[str, Any]]) -> None:
        assert self.last_parse.status == ManifestStatus.Compiling, \
            f'invalid state {self.last_parse.status}'
        self.last_parse = LastParse(
            error={'message': str(exc)},
            status=ManifestStatus.Error,
            logs=logs
        )

    def set_ready(self, logs=List[Dict[str, Any]]) -> None:
        assert self.last_parse.status == ManifestStatus.Compiling, \
            f'invalid state {self.last_parse.status}'
        self.last_parse = LastParse(
            status=ManifestStatus.Ready,
            logs=logs
        )

    def process_status(self) -> LastParse:
        with self._lock:
            last_compile = self.last_parse
        return last_compile

    def process_ps(
        self,
        active: bool = True,
        completed: bool = False,
    ) -> PSResult:
        rows = []
        now = datetime.utcnow()
        with self._lock:
            for task in self.active_tasks.values():
                row = make_task(task, now)
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
            task: RequestTaskHandler = self.active_tasks[task_id_uuid]
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
            status = KillResultStatus.Killed
            task.ended = datetime.utcnow()
            os.kill(pid, signal.SIGINT)
            task.state = TaskHandlerState.Killed
        else:
            status = KillResultStatus.Finished
            # the status must be "Completed"

        return KillResult(status)

    def process_poll(
        self,
        request_token: str,
        logs: bool = False,
        logs_start: int = 0,
    ) -> PollResult:
        task_id = uuid.UUID(request_token)
        try:
            task: RequestTaskHandler = self.active_tasks[task_id]
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
        if state <= TaskHandlerState.Running:
            return PollInProgressResult(
                status=state,
                tags=task.tags,
                logs=task_logs,
            )
        elif state == TaskHandlerState.Error:
            err = task.error
            if err is None:
                exc = dbt.exceptions.InternalException(
                    f'At end of task {task_id}, error state but error is None'
                )
                raise RPCException.from_error(
                    dbt_error(exc, logs=[l.to_dict() for l in task_logs])
                )
            # the exception has logs already attached from the child, don't
            # overwrite those
            raise err
        elif state in (TaskHandlerState.Success, TaskHandlerState.Failed):

            if task.result is None:
                exc = dbt.exceptions.InternalException(
                    f'At end of task {task_id}, state={state} but result is '
                    'None'
                )
                raise RPCException.from_error(
                    dbt_error(exc, logs=[l.to_dict() for l in task_logs])
                )
            return poll_complete(
                status=state,
                result=task.result,
                tags=task.tags,
            )
        elif state == TaskHandlerState.Killed:
            return PollKilledResult(
                status=state, tags=task.tags, logs=task_logs
            )
        else:
            exc = dbt.exceptions.InternalException(
                f'Got unknown value state={state} for task {task_id}'
            )
            raise RPCException.from_error(
                dbt_error(exc, logs=[l.to_dict() for l in task_logs])
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
            dbt.exceptions.RPCLoadException(self.last_parse.error)
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
            status = self.last_parse.status
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
        if task_id not in self.active_tasks:
            return GCResultState.Missing

        task = self.active_tasks[task_id]
        if not task.state.finished:
            return GCResultState.Running

        del self.active_tasks[task_id]
        return GCResultState.Deleted

    def _gc_task_id(self, result: GCResultSet, task_id: uuid.UUID) -> None:
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

        return result.add_result(task_id=task_id, status=status)

    def _get_gc_before_list(self, when: datetime) -> List[uuid.UUID]:
        removals: List[uuid.UUID] = []
        for task in self.active_tasks.values():
            if not task.state.finished:
                continue
            elif task.ended is None:
                continue
            elif task.ended < when:
                removals.append(task.task_id)

        return removals

    def _get_oldest_ended_list(self, num: int) -> List[uuid.UUID]:
        candidates: List[Tuple[datetime, uuid.UUID]] = []
        for task in self.active_tasks.values():
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
            self._gc_task_id(result, task_id)
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
        num_tasks = len(self.active_tasks)
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
