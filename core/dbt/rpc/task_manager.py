import multiprocessing
import os
import signal
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from functools import wraps
from typing import Any, Dict, Optional, List, Union, Set, Callable

from hologram import JsonSchemaMixin
from hologram.helpers import StrEnum

import dbt.exceptions
from dbt.rpc.error import dbt_error
from dbt.rpc.task_handler import TaskHandlerState, RequestTaskHandler
from dbt.rpc.task import RemoteCallable


@dataclass
class TaskRow(JsonSchemaMixin):
    task_id: uuid.UUID
    request_id: Union[str, int]
    request_source: str
    method: str
    state: TaskHandlerState
    start: float
    elapsed: float
    timeout: Optional[float]


class KillResultStatus(StrEnum):
    Missing = 'missing'
    NotStarted = 'not_started'
    Killed = 'killed'
    Finished = 'finished'


@dataclass
class KillResult(JsonSchemaMixin):
    state: KillResultStatus


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
        self.completed: Dict[uuid.UUID, RequestTaskHandler] = {}
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

    def process_status(self) -> JsonSchemaMixin:
        with self._lock:
            last_compile = self.last_compile
        return last_compile

    def process_ps(
        self,
        active: bool = True,
        completed: bool = False,
    ) -> JsonSchemaMixin:
        included_tasks = {}
        with self._lock:
            if completed:
                included_tasks.update(self.completed)
            if active:
                included_tasks.update(self.tasks)

        rows = []
        now = time.time()
        for task_handler in included_tasks.values():
            start = task_handler.started
            if start is not None:
                elapsed = now - start

            rows.append(TaskRow(
                task_handler.task_id, task_handler.request_id,
                task_handler.request_source, task_handler.method,
                task_handler.state, start, elapsed, task_handler.timeout
            ))
        rows.sort(key=lambda r: (r.state, r.start))
        result = PSResult(rows=rows)
        return result

    def process_kill(self, task_id: str) -> JsonSchemaMixin:
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

    def process_poll(self, task_id) -> JsonSchemaMixin:
        raise NotImplementedError('todo')

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

    def mark_done(self, request_handler):
        task_id = request_handler.task_id
        with self._lock:
            if task_id not in self.tasks:
                # lost a task! Maybe it was killed before it started.
                return
            self.completed[task_id] = self.tasks.pop(task_id)

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
