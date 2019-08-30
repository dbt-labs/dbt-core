import multiprocessing
import os
import signal
import time
import uuid
from collections import namedtuple
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, Optional, List

from hologram import JsonSchemaMixin
from hologram.helpers import StrEnum

from dbt.rpc.error import dbt_error
import dbt.exceptions


TaskRow = namedtuple(
    'TaskRow',
    'task_id request_id request_source method state start elapsed timeout'
)


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


class TaskManager:
    def __init__(self, args, config):
        self.args = args
        self.config = config
        self.tasks = {}
        self.completed = {}
        self._rpc_task_map = {}
        self._last_compile = LastCompile(status=ManifestStatus.Init)
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
            return self._last_compile.status == ManifestStatus.Ready

    def set_compiling(self):
        assert self._last_compile.status != ManifestStatus.Compiling, \
            f'invalid state {self._last_compile.status}'
        with self._lock:
            self._last_compile = LastCompile(status=ManifestStatus.Compiling)

    def set_compile_exception(self, exc, logs=List[Dict[str, Any]]):
        assert self._last_compile.status == ManifestStatus.Compiling, \
            f'invalid state {self._last_compile.status}'
        self._last_compile = LastCompile(
            error={'message': str(exc)},
            status=ManifestStatus.Error,
            logs=logs
        )

    def set_ready(self, logs=List[Dict[str, Any]]):
        assert self._last_compile.status == ManifestStatus.Compiling, \
            f'invalid state {self._last_compile.status}'
        self._last_compile = LastCompile(
            status=ManifestStatus.Ready,
            logs=logs
        )

    def process_status(self):
        with self._lock:
            last_compile = self._last_compile

        status = last_compile.to_dict()
        status['pid'] = os.getpid()
        return status

    def process_listing(self, active=True, completed=False):
        included_tasks = {}
        with self._lock:
            if completed:
                included_tasks.update(self.completed)
            if active:
                included_tasks.update(self.tasks)

        table = []
        now = time.time()
        for task_handler in included_tasks.values():
            start = task_handler.started
            if start is not None:
                elapsed = now - start

            table.append(TaskRow(
                str(task_handler.task_id), task_handler.request_id,
                task_handler.request_source, task_handler.method,
                task_handler.state, start, elapsed, task_handler.timeout
            ))
        table.sort(key=lambda r: (r.state, r.start))
        result = {
            'rows': [dict(r._asdict()) for r in table],
        }
        return result

    def process_kill(self, task_id):
        # TODO: this result design is terrible
        result = {
            'found': False,
            'started': False,
            'finished': False,
            'killed': False
        }
        task_id = uuid.UUID(task_id)
        try:
            task = self.tasks[task_id]
        except KeyError:
            # nothing to do!
            return result

        result['found'] = True

        if task.process is None:
            return result
        pid = task.process.pid
        if pid is None:
            return result

        result['started'] = True

        if task.process.is_alive():
            os.kill(pid, signal.SIGINT)
            result['killed'] = True
            return result

        result['finished'] = True
        return result

    def process_currently_compiling(self, *args, **kwargs):
        raise dbt_error(dbt.exceptions.RPCCompiling('compile in progress'))

    def process_compilation_error(self, *args, **kwargs):
        raise dbt_error(
            dbt.exceptions.RPCLoadException(self._last_compile.error)
        )

    def rpc_builtin(self, method_name):
        if method_name == 'ps':
            return self.process_listing
        if method_name == 'kill' and os.name != 'nt':
            return self.process_kill
        if method_name == 'status':
            return self.process_status
        if method_name in self._rpc_task_map:
            if self._last_compile.status == ManifestStatus.Compiling:
                return self.process_currently_compiling
            if self._last_compile.status == ManifestStatus.Error:
                return self.process_compilation_error
        return None

    def mark_done(self, request_handler):
        task_id = request_handler.task_id
        with self._lock:
            if task_id not in self.tasks:
                # lost a task! Maybe it was killed before it started.
                return
            self.completed[task_id] = self.tasks.pop(task_id)

    def methods(self):
        rpc_builtin_methods = ['ps', 'status']
        if os.name != 'nt':
            rpc_builtin_methods.append('kill')

        with self._lock:
            task_map = list(self._rpc_task_map)

        return task_map + rpc_builtin_methods
