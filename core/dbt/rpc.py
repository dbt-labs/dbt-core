from jsonrpc.exceptions import \
    JSONRPCDispatchException, \
    JSONRPCInvalidParams, \
    JSONRPCParseError, \
    JSONRPCInvalidRequestException, \
    JSONRPCInvalidRequest
from jsonrpc import JSONRPCResponseManager
from jsonrpc.jsonrpc import JSONRPCRequest
from jsonrpc.jsonrpc2 import JSONRPC20Response

import json
import uuid
import multiprocessing
import os
import signal
import time
from collections import namedtuple

from dbt.adapters.factory import load_plugin
from dbt.compat import QueueEmpty
from dbt.logger import RPC_LOGGER as logger
from dbt.logger import add_queue_handler
import dbt.exceptions


class RPCException(JSONRPCDispatchException):
    def __init__(self, code=None, message=None, data=None, logs=None):
        if code is None:
            code = -32000
        if message is None:
            message = 'Server error'
        if data is None:
            data = {}

        super(RPCException, self).__init__(code=code, message=message,
                                           data=data)
        self.logs = logs

    def __str__(self):
        return (
            'RPCException({0.code}, {0.message}, {0.data}, {1.logs})'
            .format(self.error, self)
        )

    @property
    def logs(self):
        return self.error.data.get('logs')

    @logs.setter
    def logs(self, value):
        if value is None:
            return
        self.error.data['logs'] = value

    @classmethod
    def from_error(cls, err):
        return cls(err.code, err.message, err.data, err.data.get('logs'))


def invalid_params(data):
    return RPCException(
        code=JSONRPCInvalidParams.CODE,
        message=JSONRPCInvalidParams.MESSAGE,
        data=data
    )


def server_error(err, logs=None):
    exc = dbt.exceptions.Exception(str(err))
    return dbt_error(exc, logs)


def timeout_error(timeout_value, logs=None):
    exc = dbt.exceptions.RPCTimeoutException(timeout_value)
    return dbt_error(exc, logs)


def dbt_error(exc, logs=None):
    exc = RPCException(code=exc.CODE, message=exc.MESSAGE, data=exc.data(),
                       logs=logs)
    return exc


class QueueMessageType(object):
    Error = 'error'
    Result = 'result'
    Log = 'log'

    @classmethod
    def terminating(cls):
        return [
            cls.Error,
            cls.Result
        ]


def sigterm_handler(signum, frame):
    raise dbt.exceptions.RPCKilledException(signum)


class RequestDispatcher(object):
    """A special dispatcher that knows about requests."""
    def __init__(self, http_request, json_rpc_request, manager):
        self.http_request = http_request
        self.json_rpc_request = json_rpc_request
        self.manager = manager
        self.task = None

    def rpc_factory(self, task):
        request_handler = RequestTaskHandler(task,
                                             self.http_request,
                                             self.json_rpc_request)

        def rpc_func(**kwargs):
            try:
                self.manager.add_request(request_handler)
                return request_handler.handle(kwargs)
            finally:
                self.manager.mark_done(request_handler)

        return rpc_func

    def __getitem__(self, key):
        # the dispatcher's keys are method names and its values are functions
        # that implement the RPC calls
        func = self.manager.rpc_builtin(key)
        if func is not None:
            return func

        task = self.manager.rpc_task(key)
        return self.rpc_factory(task)


def _nt_setup(config, args):
    """
    On windows, we have to do a some things that dbt does dynamically at
    process load.

    These things are inherited automatically on posix, where fork() keeps
    everything in memory.
    """
    # reload the active plugin
    load_plugin(config.credentials.type)

    # reset tracking, etc
    config.config.set_values(args.profiles_dir)


def _task_bootstrap(task, queue, kwargs):
    signal.signal(signal.SIGTERM, sigterm_handler)
    # the first thing we do in a new process: start logging
    add_queue_handler(queue)
    # on windows, we need to reload our plugins because of how it starts new
    # processes. At this point there are no adapter plugins loaded!
    if os.name == 'nt':
        _nt_setup(task.config, task.args)

    error = None
    result = None
    try:
        result = task.handle_request(**kwargs)
    except RPCException as exc:
        error = exc
    except dbt.exceptions.RPCKilledException as exc:
        # do NOT log anything here, you risk triggering a deadlock on the
        # queue handler we inserted above
        error = dbt_error(exc)
    except dbt.exceptions.Exception as exc:
        logger.debug('dbt runtime exception', exc_info=True)
        error = dbt_error(exc)
    except Exception as exc:
        logger.debug('uncaught python exception', exc_info=True)
        error = server_error(exc)

    # put whatever result we got onto the queue as well.
    if error is not None:
        queue.put([QueueMessageType.Error, error.error])
    else:
        queue.put([QueueMessageType.Result, result])


class RequestTaskHandler(object):
    def __init__(self, task, http_request, json_rpc_request):
        self.task = task
        self.http_request = http_request
        self.json_rpc_request = json_rpc_request
        self.queue = None
        self.process = None
        self.started = None
        self.timeout = None
        self.logs = []
        self.task_id = uuid.uuid4()

    @property
    def request_source(self):
        return self.http_request.remote_addr

    @property
    def request_id(self):
        return self.json_rpc_request._id

    @property
    def method(self):
        return self.task.METHOD_NAME

    def _next_timeout(self):
        if self.timeout is None:
            return None
        end = self.started + self.timeout
        timeout = end - time.time()
        if timeout < 0:
            raise dbt.exceptions.RPCTimeoutException(self.timeout)
        return timeout

    def _wait_for_results(self):
        """Wait for results off the queue. If there is a timeout set, and it is
        exceeded, raise an RPCTimeoutException.
        """
        while True:
            get_timeout = self._next_timeout()
            try:
                msgtype, value = self.queue.get(timeout=get_timeout)
            except QueueEmpty:
                raise dbt.exceptions.RPCTimeoutException(self.timeout)

            if msgtype == QueueMessageType.Log:
                self.logs.append(value)
            elif msgtype in QueueMessageType.terminating():
                return msgtype, value
            else:
                raise dbt.exceptions.InternalException(
                    'Got invalid queue message type {}'.format(msgtype)
                )

    def _join_process(self):
        try:
            msgtype, result = self._wait_for_results()
        except dbt.exceptions.RPCTimeoutException:
            self.process.terminate()
            raise timeout_error(self.timeout)
        except dbt.exceptions.Exception as exc:
            raise dbt_error(exc)
        except Exception as exc:
            raise server_error(exc)
        finally:
            self.process.join()

        if msgtype == QueueMessageType.Error:
            raise RPCException.from_error(result)

        return result

    def get_result(self):
        try:
            result = self._join_process()
        except RPCException as exc:
            exc.logs = self.logs
            raise

        result['logs'] = self.logs
        return result

    def handle(self, kwargs):
        self.started = time.time()
        self.timeout = kwargs.pop('timeout', None)
        self.queue = multiprocessing.Queue()
        self.process = multiprocessing.Process(
            target=_task_bootstrap,
            args=(self.task, self.queue, kwargs)
        )
        self.process.start()
        result = self.get_result()
        return result

    @property
    def state(self):
        if self.started is None:
            return 'not started'
        elif self.process is None:
            return 'initializing'
        elif self.process.is_alive():
            return 'running'
        else:
            return 'finished'


TaskRow = namedtuple(
    'TaskRow',
    'task_id request_id request_source method state start elapsed timeout'
)


class TaskManager(object):
    def __init__(self):
        self.tasks = {}
        self.completed = {}
        self._rpc_task_map = {}
        self._rpc_function_map = {}
        self._lock = multiprocessing.Lock()

    def add_request(self, request_handler):
        self.tasks[request_handler.task_id] = request_handler

    def add_task_handler(self, task):
        self._rpc_task_map[task.METHOD_NAME] = task

    def rpc_task(self, method_name):
        return self._rpc_task_map[method_name]

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

    def rpc_builtin(self, method_name):
        if method_name == 'ps':
            return self.process_listing
        if method_name == 'kill' and os.name != 'nt':
            return self.process_kill
        return None

    def mark_done(self, request_handler):
        task_id = request_handler.task_id
        with self._lock:
            if task_id not in self.tasks:
                # lost a task! Maybe it was killed before it started.
                return
            self.completed[task_id] = self.tasks.pop(task_id)

    def methods(self):
        rpc_builtin_methods = ['ps']
        if os.name != 'nt':
            rpc_builtin_methods.append('kill')
        return list(self._rpc_task_map) + rpc_builtin_methods


class ResponseManager(JSONRPCResponseManager):
    """Override the default response manager to handle request metadata and
    track in-flight tasks.
    """
    @classmethod
    def handle(cls, http_request, task_manager):
        # pretty much just copy+pasted from the original, with slight tweaks to
        # preserve the request
        request_str = http_request.data
        if isinstance(request_str, bytes):
            request_str = request_str.decode("utf-8")

        try:
            data = json.loads(request_str)
        except (TypeError, ValueError):
            return JSONRPC20Response(error=JSONRPCParseError()._data)

        try:
            request = JSONRPCRequest.from_data(data)
        except JSONRPCInvalidRequestException:
            return JSONRPC20Response(error=JSONRPCInvalidRequest()._data)

        dispatcher = RequestDispatcher(
            http_request,
            request,
            task_manager
        )

        return cls.handle_request(request, dispatcher)
