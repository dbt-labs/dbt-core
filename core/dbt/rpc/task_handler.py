import multiprocessing
import signal
import sys
import threading
import uuid
from datetime import datetime
from typing import Any, Dict, Union, Optional, List, Type

from hologram import JsonSchemaMixin, ValidationError
from hologram.helpers import StrEnum

import dbt.exceptions
import dbt.flags
from dbt.adapters.factory import (
    cleanup_connections, load_plugin, register_adapter
)
from dbt.contracts.rpc import RPCParameters, RemoteResult
from dbt.logger import (
    GLOBAL_LOGGER as logger, list_handler, LogMessage, OutputHandler
)
from dbt.rpc.error import (
    dbt_error,
    server_error,
    RPCException,
    timeout_error,
)
from dbt.rpc.logger import (
    QueueSubscriber,
    QueueLogHandler,
    QueueErrorMessage,
    QueueResultMessage,
    QueueTimeoutMessage,
)
from dbt.rpc.method import RemoteMethod
from dbt.utils import env_set_truthy

# we use this in typing only...
from queue import Queue  # noqa


SINGLE_THREADED_HANDLER = env_set_truthy('DBT_SINGLE_THREADED_HANDLER')


class TaskHandlerState(StrEnum):
    NotStarted = 'not started'
    Initializing = 'initializing'
    Running = 'running'
    Success = 'success'
    Error = 'error'

    def __lt__(self, other) -> bool:
        """A logical ordering for TaskHandlerState:

        NotStarted < Initializing < Running < (Success, Error)
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
        return self == self.Error or self == self.Success


def sigterm_handler(signum, frame):
    raise dbt.exceptions.RPCKilledException(signum)


def _spawn_setup(task):
    """
    Because we're using spawn, we have to do a some things that dbt does
    dynamically at process load.

    These things are inherited automatically in fork mode, where fork() keeps
    everything in memory.
    """
    # reset flags
    dbt.flags.set_from_args(task.args)
    # reload the active plugin
    load_plugin(task.config.credentials.type)
    # register it
    register_adapter(task.config)

    # reset tracking, etc
    task.config.config.set_values(task.args.profiles_dir)


def _task_bootstrap(
    task: RemoteMethod,
    queue,  # typing: Queue[Tuple[QueueMessageType, Any]]
    params: JsonSchemaMixin,
) -> None:
    """_task_bootstrap runs first inside the child process"""
    signal.signal(signal.SIGTERM, sigterm_handler)
    # the first thing we do in a new process: push logging back over our queue
    handler = QueueLogHandler(queue)
    with handler.applicationbound():
        _spawn_setup(task)
        rpc_exception = None
        result = None
        try:
            task.set_args(params=params)
            result = task.handle_request()
        except RPCException as exc:
            rpc_exception = exc
        except dbt.exceptions.RPCKilledException as exc:
            # do NOT log anything here, you risk triggering a deadlock on the
            # queue handler we inserted above
            rpc_exception = dbt_error(exc)
        except dbt.exceptions.Exception as exc:
            logger.debug('dbt runtime exception', exc_info=True)
            rpc_exception = dbt_error(exc)
        except Exception as exc:
            with OutputHandler(sys.stderr).applicationbound():
                logger.error('uncaught python exception', exc_info=True)
            rpc_exception = server_error(exc)

        # put whatever result we got onto the queue as well.
        if rpc_exception is not None:
            handler.emit_error(rpc_exception.error)
        elif result is not None:
            handler.emit_result(result)
        else:
            error = dbt_error(dbt.exceptions.InternalException(
                'after request handling, neither result nor error is None!'
            ))
            handler.emit_error(error.error)


class StateHandler:
    """A helper context manager to manage task handler state."""
    def __init__(self, task_handler: 'RequestTaskHandler') -> None:
        self.handler = task_handler

    def __enter__(self) -> None:
        return None

    def set_end(self):
        self.handler.ended = datetime.utcnow()

    def handle_success(self):
        self.handler.state = TaskHandlerState.Success
        self.set_end()

    def handle_error(self, exc_type, exc_value, exc_tb) -> bool:
        if isinstance(exc_value, RPCException):
            self.handler.error = exc_value
            self.handler.state = TaskHandlerState.Error
        elif isinstance(exc_value, dbt.exceptions.Exception):
            self.handler.error = dbt_error(exc_value)
            self.handler.state = TaskHandlerState.Error
        else:
            # we should only get here if we got a BaseException that is not
            # an Exception (we caught those in _wait_for_results), or a bug
            # in get_result's call stack. Either way, we should set an
            # error so we can figure out what happened on thread death
            self.handler.error = server_error(exc_value)
            self.handler.state = TaskHandlerState.Error
        self.set_end()
        return False

    def __exit__(self, exc_type, exc_value, exc_tb) -> bool:
        if exc_type is not None:
            return self.handle_error(exc_type, exc_value, exc_tb)

        self.handle_success()
        return False


class ErrorOnlyStateHandler(StateHandler):
    """A state handler that does not touch state on success."""
    def handle_success(self):
        pass


class RequestTaskHandler(threading.Thread):
    """Handler for the single task triggered by a given jsonrpc request."""
    def __init__(self, manager, task, http_request, json_rpc_request):
        self.manager = manager
        self.task = task
        self.http_request = http_request
        self.json_rpc_request = json_rpc_request
        self.subscriber: Optional[QueueSubscriber] = None
        self.process: Optional[multiprocessing.Process] = None
        self.thread: Optional[threading.Thread] = None
        self.started: Optional[datetime] = None
        self.ended: Optional[datetime] = None
        self.task_id: uuid.UUID = uuid.uuid4()
        # the are multiple threads potentially operating on these attributes:
        #   - the task manager has the RequestTaskHandler and any requests
        #     might access it via ps/kill, but only for reads
        #   - The actual thread that this represents, which writes its data to
        #     the result and logs. The atomicity of list.append() and item
        #     assignment means we don't need a lock.
        self.result: Optional[JsonSchemaMixin] = None
        self.error: Optional[RPCException] = None
        self.state: TaskHandlerState = TaskHandlerState.NotStarted
        self.logs: List[LogMessage] = []
        self.task_kwargs: Optional[Dict[str, Any]] = None
        self.task_params: Optional[RPCParameters] = None
        super().__init__(
            name='{}-handler-{}'.format(self.task_id, self.method),
            daemon=True,  # if the RPC server goes away, we probably should too
        )

    @property
    def request_source(self) -> str:
        return self.http_request.remote_addr

    @property
    def request_id(self) -> Union[str, int]:
        return self.json_rpc_request._id

    @property
    def method(self) -> str:
        return self.task.METHOD_NAME

    @property
    def _single_threaded(self):
        return self.task.args.single_threaded or SINGLE_THREADED_HANDLER

    @property
    def timeout(self) -> Optional[float]:
        if self.task_params is None or self.task_params.timeout is None:
            return None
        # task_params.timeout is a `Real` for encoding reasons, but we just
        # want it as a float.
        return float(self.task_params.timeout)

    @property
    def tags(self) -> Optional[Dict[str, Any]]:
        if self.task_params is None:
            return None
        return self.task_params.task_tags

    def _wait_for_results(self) -> RemoteResult:
        """Wait for results off the queue. If there is an exception raised,
        raise an appropriate RPC exception.

        This does not handle joining, but does terminate the process if it
        timed out.
        """
        if (
            self.subscriber is None or
            self.started is None or
            self.process is None
        ):
            raise dbt.exceptions.InternalException(
                '_wait_for_results() called before handle()'
            )

        try:
            msg = self.subscriber.dispatch_until_exit(
                started=self.started,
                timeout=self.timeout,
            )
        except dbt.exceptions.Exception as exc:
            raise dbt_error(exc)
        except Exception as exc:
            raise server_error(exc)
        if isinstance(msg, QueueErrorMessage):
            raise RPCException.from_error(msg.error)
        elif isinstance(msg, QueueTimeoutMessage):
            if not self._single_threaded:
                self.process.terminate()
            raise timeout_error(self.timeout)
        elif isinstance(msg, QueueResultMessage):
            return msg.result
        else:
            raise dbt.exceptions.InternalException(
                'Invalid message type {} (result={})'.format(msg)
            )

    def get_result(self) -> RemoteResult:
        if self.process is None:
            raise dbt.exceptions.InternalException(
                'get_result() called before handle()'
            )

        try:
            with list_handler(self.logs):
                try:
                    result = self._wait_for_results()
                finally:
                    if not self._single_threaded:
                        self.process.join()
        except RPCException as exc:
            # RPC Exceptions come already preserialized for the jsonrpc
            # framework
            exc.logs = [l.to_dict() for l in self.logs]
            exc.tags = self.tags
            raise

        # results get real logs
        result.logs = self.logs[:]
        return result

    def run(self):
        try:
            with StateHandler(self):
                self.result = self.get_result()
        except RPCException:
            pass  # rpc exceptions are fine, the managing thread will handle it

    def handle_singlethreaded(self, kwargs):
        # in single-threaded mode, we're going to remain synchronous, so call
        # `run`, not `start`, and return an actual result.
        # note this shouldn't call self.run() as that has different semantics
        # (we want errors to raise)
        self.process.run()
        with StateHandler(self):
            self.result = self.get_result()
        return self.result

    def start(self):
        # this is pretty unfortunate, but we have to reset the adapter
        # cache _before_ we fork on posix. libpq, but also any other
        # adapters that rely on file descriptors, get really messed up if
        # you fork(), because the fds get inherited but the state isn't
        # shared. The child process and the parent might end up trying to
        # do things on the same fd at the same time.
        # Also for some reason, if you do this after forking, even without
        # calling close(), the connection in the parent ends up throwing
        # 'connection already closed' exceptions
        cleanup_connections()
        self.process.start()
        self.state = TaskHandlerState.Running
        super().start()

    def _collect_parameters(self):
        # both get_parameters and the argparse can raise a TypeError.
        cls: Type[RPCParameters] = self.task.get_parameters()

        try:
            return cls.from_dict(self.task_kwargs)
        except ValidationError as exc:
            # raise a TypeError to indicate invalid parameters so we get a nice
            # error from our json-rpc library
            raise TypeError(exc) from exc

    def handle(self, kwargs: Dict[str, Any]) -> Dict[str, Any]:
        self.started = datetime.utcnow()
        self.state = TaskHandlerState.Initializing
        self.task_kwargs = kwargs
        with ErrorOnlyStateHandler(self):
            # this will raise a TypeError if you provided bad arguments.
            self.task_params = self._collect_parameters()
        if self.task_params is None:
            raise dbt.exceptions.InternalException(
                'Task params set to None!'
            )
        self.subscriber = QueueSubscriber(dbt.flags.MP_CONTEXT.Queue())
        self.process = dbt.flags.MP_CONTEXT.Process(
            target=_task_bootstrap,
            args=(self.task, self.subscriber.queue, self.task_params)
        )

        if self._single_threaded:
            # all requests are synchronous in single-threaded mode. No need to
            # create a process...
            return self.handle_singlethreaded(kwargs)

        self.start()
        return {'request_token': str(self.task_id)}

    def __call__(self, **kwargs) -> Dict[str, Any]:
        # __call__ happens deep inside jsonrpc's framework
        self.manager.add_request(self)
        return self.handle(kwargs)
