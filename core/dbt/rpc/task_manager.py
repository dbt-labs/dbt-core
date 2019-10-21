import threading
import uuid
from datetime import datetime
from typing import (
    Any, Dict, Optional, List, Union, Set, Callable, Type, MutableMapping
)


import dbt.exceptions
import dbt.flags
from dbt.contracts.graph.manifest import Manifest
from dbt.contracts.rpc import (
    LastParse,
    ManifestStatus,
    GCSettings,
    GCResult,
    TaskRow,
    TaskHandlerState,
    TaskID,
)
from dbt.logger import LogMessage, list_handler
from dbt.perf_utils import get_full_manifest
from dbt.rpc.error import dbt_error
from dbt.rpc.gc import GarbageCollector, Collectible
from dbt.rpc.task_handler import (
    RequestTaskHandler, set_parse_state_with
)
from dbt.rpc.method import (
    RemoteMethod, RemoteManifestMethod, RemoteBuiltinMethod, TaskTypes,
)
# pick up our builtin methods
import dbt.rpc.builtins  # noqa


# import this to make sure our timedelta encoder is registered
from dbt import helper_types  # noqa
from dbt.utils import env_set_truthy


SINGLE_THREADED_WEBSERVER = env_set_truthy('DBT_SINGLE_THREADED_WEBSERVER')


WrappedHandler = Callable[..., Dict[str, Any]]

TaskDict = MutableMapping[uuid.UUID, RequestTaskHandler]


def _assert_started(task_handler: Collectible) -> datetime:
    if task_handler.started is None:
        raise dbt.exceptions.InternalException(
            'task handler started but start time is not set'
        )
    return task_handler.started


def _assert_ended(task_handler: Collectible) -> datetime:
    if task_handler.ended is None:
        raise dbt.exceptions.InternalException(
            'task handler finished but end time is not set'
        )
    return task_handler.ended


def make_task(task_handler: Collectible, now_time: datetime) -> TaskRow:
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


class UnconditionalError:
    def __init__(self, exception: dbt.exceptions.Exception):
        self.exception = dbt_error(exception)

    def __call__(self, *args, **kwargs):
        raise self.exception


class ParseError(UnconditionalError):
    def __init__(self, parse_error):
        exception = dbt.exceptions.RPCLoadException(parse_error)
        super().__init__(exception)


class CurrentlyCompiling(UnconditionalError):
    def __init__(self):
        exception = dbt.exceptions.RPCCompiling('compile in progress')
        super().__init__(exception)


class ManifestReloader(threading.Thread):
    def __init__(self, task_manager: 'TaskManager') -> None:
        super().__init__()
        self.task_manager = task_manager

    def reload_manifest(self):
        logs: List[LogMessage] = []
        with set_parse_state_with(self.task_manager, lambda: logs):
            with list_handler(logs):
                self.task_manager.parse_manifest()

    def run(self) -> None:
        try:
            self.reload_manifest()
        except Exception:
            # ignore ugly thread-death error messages to stderr
            pass


class TaskManager:
    def __init__(self, args, config, task_types: TaskTypes) -> None:
        self.args = args
        self.config = config
        self.manifest: Optional[Manifest] = None
        self._task_types: TaskTypes = task_types
        self.active_tasks: MutableMapping[uuid.UUID, Collectible] = {}
        self.gc = GarbageCollector(active_tasks=self.active_tasks)
        self.last_parse: LastParse = LastParse(status=ManifestStatus.Init)
        self._lock: dbt.flags.MP_CONTEXT.Lock = dbt.flags.MP_CONTEXT.Lock()
        self._reloader: Optional[ManifestReloader] = None
        self.reload_manifest()

    def single_threaded(self):
        return SINGLE_THREADED_WEBSERVER or self.args.single_threaded

    def _reload_task_manager_thread(self, reloader: ManifestReloader):
        """This function can only be running once at a time, as it runs in the
        signal handler we replace
        """
        # compile in a thread that will fix up the tag manager when it's done
        reloader.start()
        # only assign to _reloader here, to avoid calling join() before start()
        self._reloader = reloader

    def _reload_task_manager_fg(self, reloader: ManifestReloader):
        """Override for single-threaded mode to run in the foreground"""
        # just reload directly
        reloader.reload_manifest()

    def reload_manifest(self) -> bool:
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

    def reload_builtin_tasks(self):
        # reload all the non-manifest tasks because the config changed.
        # manifest tasks are still blocked so we can ignore them
        for task_cls in self._task_types.builtin():
            self.add_builtin_task_handler(task_cls)

    def reload_config(self):
        config = self.config.from_args(self.args)
        self.config = config
        return config

    def add_request(self, request_handler: RequestTaskHandler):
        self.active_tasks[request_handler.task_id] = request_handler

    def get_request(self, task_id: TaskID) -> Collectible:
        try:
            return self.active_tasks[task_id]
        except KeyError:
            # We don't recognize that ID.
            raise dbt.exceptions.UnknownAsyncIDException(task_id) from None

    def _get_manifest_callable(
        self, task: Type[RemoteManifestMethod]
    ) -> Union[UnconditionalError, RemoteManifestMethod]:
        status = self.last_parse.status
        if status == ManifestStatus.Compiling:
            return CurrentlyCompiling()
        elif status == ManifestStatus.Error:
            return ParseError(self.last_parse.error)
        else:
            if self.manifest is None:
                raise dbt.exceptions.InternalException(
                    f'Manifest should not be None if the last parse status is '
                    f'{status}'
                )
            return task(self.args, self.config, self.manifest)

    def rpc_task(
        self, method_name: str
    ) -> Union[UnconditionalError, RemoteMethod]:
        with self._lock:
            task = self._task_types[method_name]
            if issubclass(task, RemoteBuiltinMethod):
                return task(self)
            elif issubclass(task, RemoteManifestMethod):
                return self._get_manifest_callable(task)
            elif issubclass(task, RemoteMethod):
                return task(self.args, self.config)
            else:
                raise dbt.exceptions.InternalException(
                    f'Got a task with an invalid type! {task} with method '
                    f'name {method_name} has a type of {task.__class__}, '
                    f'should be a RemoteMethod'
                )

    def ready(self) -> bool:
        with self._lock:
            return self.last_parse.status == ManifestStatus.Ready

    def set_parsing(self) -> bool:
        with self._lock:
            if self.last_parse.status == ManifestStatus.Compiling:
                return False
            self.last_parse = LastParse(status=ManifestStatus.Compiling)
        return True

    def parse_manifest(self) -> None:
        self.manifest = get_full_manifest(self.config)

    def set_compile_exception(self, exc, logs=List[LogMessage]) -> None:
        assert self.last_parse.status == ManifestStatus.Compiling, \
            f'invalid state {self.last_parse.status}'
        self.last_parse = LastParse(
            error={'message': str(exc)},
            status=ManifestStatus.Error,
            logs=logs
        )

    def set_ready(self, logs=List[LogMessage]) -> None:
        assert self.last_parse.status == ManifestStatus.Compiling, \
            f'invalid state {self.last_parse.status}'
        self.last_parse = LastParse(
            status=ManifestStatus.Ready,
            logs=logs
        )

    def methods(self) -> Set[str]:
        with self._lock:
            return set(self._task_types)

    def currently_compiling(self, *args, **kwargs):
        """Raise an RPC exception to trigger the error handler."""
        raise dbt_error(dbt.exceptions.RPCCompiling('compile in progress'))

    def compilation_error(self, *args, **kwargs):
        """Raise an RPC exception to trigger the error handler."""
        raise dbt_error(
            dbt.exceptions.RPCLoadException(self.last_parse.error)
        )

    def get_handler(
        self, method, http_request, json_rpc_request
    ) -> Optional[Union[WrappedHandler, RemoteMethod]]:
        # get_handler triggers a GC check. TODO: does this go somewhere else?
        self.gc_as_required()

        if method not in self._task_types:
            return None

        task = self.rpc_task(method)
        return task

    def task_table(self) -> List[TaskRow]:
        rows: List[TaskRow] = []
        now = datetime.utcnow()
        with self._lock:
            for task in self.active_tasks.values():
                rows.append(make_task(task, now))
        return rows

    def gc_as_required(self) -> None:
        with self._lock:
            return self.gc.collect_as_required()

    def gc_safe(
        self,
        task_ids: Optional[List[uuid.UUID]] = None,
        before: Optional[datetime] = None,
        settings: Optional[GCSettings] = None,
    ) -> GCResult:
        with self._lock:
            return self.gc.collect_selected(
                task_ids=task_ids, before=before, settings=settings,
            )
