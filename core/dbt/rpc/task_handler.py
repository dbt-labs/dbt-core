import signal
import sys
import threading
import uuid
from contextlib import contextmanager
from datetime import datetime
from typing import (
    Any, Dict, Union, Optional, List, Type, Callable, Iterator
)
from typing_extensions import Protocol

from hologram import JsonSchemaMixin, ValidationError

import dbt.exceptions
import dbt.flags
from dbt.adapters.factory import (
    cleanup_connections, load_plugin, register_adapter,
)
from dbt.contracts.rpc import (
    RPCParameters, RemoteResult, TaskHandlerState, RemoteMethodFlags, TaskTags,
)
from dbt.exceptions import InternalException
from dbt.logger import (
    GLOBAL_LOGGER as logger, list_handler, LogMessage, OutputHandler,
)
from dbt.rpc.error import (
    dbt_error,
    server_error,
    RPCException,
    timeout_error,
)
from dbt.rpc.task_handler_protocol import TaskHandlerProtocol
from dbt.rpc.logger import (
    QueueSubscriber,
    QueueLogHandler,
    QueueErrorMessage,
    QueueResultMessage,
    QueueTimeoutMessage,
)
from dbt.rpc.method import RemoteMethod


# we use this in typing only...
from queue import Queue  # noqa


def sigterm_handler(signum, frame):
    raise dbt.exceptions.RPCKilledException(signum)


class BootstrapProcess(dbt.flags.MP_CONTEXT.Process):
    def __init__(
        self,
        task: RemoteMethod,
        queue,  # typing: Queue[Tuple[QueueMessageType, Any]]
    ) -> None:
        self.task = task
        self.queue = queue
        super().__init__()

    def _spawn_setup(self):
        """
        Because we're using spawn, we have to do a some things that dbt does
        dynamically at process load.

        These things are inherited automatically in fork mode, where fork()
        keeps everything in memory.
        """
        # reset flags
        dbt.flags.set_from_args(self.task.args)
        # reload the active plugin
        load_plugin(self.task.config.credentials.type)
        # register it
        register_adapter(self.task.config)

        # reset tracking, etc
        self.task.config.config.set_values(self.task.args.profiles_dir)

    def task_exec(self) -> None:
        """task_exec runs first inside the child process"""
        signal.signal(signal.SIGTERM, sigterm_handler)
        # the first thing we do in a new process: push logging back over our
        # queue
        handler = QueueLogHandler(self.queue)
        with handler.applicationbound():
            self._spawn_setup()
            # copy threads over into our credentials, if it exists and is set.
            # some commands, like 'debug', won't have a threads value at all.
            if getattr(self.task.args, 'threads', None) is not None:
                self.task.config.threads = self.task.args.threads
            rpc_exception = None
            result = None
            try:
                result = self.task.handle_request()
            except RPCException as exc:
                rpc_exception = exc
            except dbt.exceptions.RPCKilledException as exc:
                # do NOT log anything here, you risk triggering a deadlock on
                # the queue handler we inserted above
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
                error = dbt_error(InternalException(
                    'after request handling, neither result nor error is None!'
                ))
                handler.emit_error(error.error)

    def run(self):
        self.task_exec()


class TaskManagerProtocol(Protocol):
    config: Any

    def set_parsing(self):
        pass

    def set_compile_exception(
        self, exc: Exception, logs: List[LogMessage]
    ):
        pass

    def set_ready(self, logs: List[LogMessage]):
        pass

    def add_request(self, request: 'RequestTaskHandler') -> Dict[str, Any]:
        pass

    def parse_manifest(self):
        pass

    def reload_config(self):
        pass


@contextmanager
def set_parse_state_with(
    manager: TaskManagerProtocol,
    logs: Callable[[], List[LogMessage]],
) -> Iterator[None]:
    """Given a task manager and either a list of logs or a callable that
    returns said list, set appropriate state on the manager upon exiting.
    """
    try:
        yield
    except Exception as exc:
        manager.set_compile_exception(exc, logs=logs())
        raise
    else:
        manager.set_ready(logs=logs())


@contextmanager
def _noop_context() -> Iterator[None]:
    yield


@contextmanager
def get_results_context(
    flags: RemoteMethodFlags,
    manager: TaskManagerProtocol,
    logs: Callable[[], List[LogMessage]]
) -> Iterator[None]:

    if RemoteMethodFlags.BlocksManifestTasks in flags:
        manifest_blocking = set_parse_state_with(manager, logs)
    else:
        manifest_blocking = _noop_context()

    with manifest_blocking:
        yield
        if RemoteMethodFlags.RequiresManifestReloadAfter in flags:
            manager.parse_manifest()


class StateHandler:
    """A helper context manager to manage task handler state."""
    def __init__(self, task_handler: 'RequestTaskHandler') -> None:
        self.handler = task_handler

    def __enter__(self) -> None:
        return None

    def set_end(self):
        self.handler.ended = datetime.utcnow()

    def handle_completed(self):
        # killed handlers don't get a result.
        if self.handler.state != TaskHandlerState.Killed:
            if self.handler.result is None:
                # there wasn't an error before, but there sure is one now
                self.handler.error = dbt_error(
                    InternalException(
                        'got an invalid result=None, but state was {}'
                        .format(self.handler.state)
                    )
                )
            elif self.handler.task.interpret_results(self.handler.result):
                self.handler.state = TaskHandlerState.Success
            else:
                self.handler.state = TaskHandlerState.Failed
        self.set_end()

    def handle_error(self, exc_type, exc_value, exc_tb) -> bool:
        if isinstance(exc_value, RPCException):
            self.handler.error = exc_value
        elif isinstance(exc_value, dbt.exceptions.Exception):
            self.handler.error = dbt_error(exc_value)
        else:
            # we should only get here if we got a BaseException that is not
            # an Exception (we caught those in _wait_for_results), or a bug
            # in get_result's call stack. Either way, we should set an
            # error so we can figure out what happened on thread death
            self.handler.error = server_error(exc_value)
        if self.handler.state != TaskHandlerState.Killed:
            self.handler.state = TaskHandlerState.Error
        self.set_end()
        return False

    def task_teardown(self):
        self.handler.task.cleanup(self.handler.result)

    def __exit__(self, exc_type, exc_value, exc_tb) -> None:
        try:
            if exc_type is not None:
                self.handle_error(exc_type, exc_value, exc_tb)
            else:
                self.handle_completed()
            return
        finally:
            # we really really promise to run your teardown
            self.task_teardown()


class SetArgsStateHandler(StateHandler):
    """A state handler that does not touch state on success and does not
    execute the teardown
    """
    def handle_completed(self):
        pass

    def handle_teardown(self):
        pass


class RequestTaskHandler(threading.Thread, TaskHandlerProtocol):
    """Handler for the single task triggered by a given jsonrpc request."""
    def __init__(
        self,
        manager: TaskManagerProtocol,
        task: RemoteMethod,
        http_request,
        json_rpc_request,
    ) -> None:
        self.manager: TaskManagerProtocol = manager
        self.task: RemoteMethod = task
        self.http_request = http_request
        self.json_rpc_request = json_rpc_request
        self.subscriber: Optional[QueueSubscriber] = None
        self.process: Optional[BootstrapProcess] = None
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
        if self.task.METHOD_NAME is None:  # mypy appeasement
            raise InternalException(
                f'In the request handler, got a task({self.task}) with no '
                'METHOD_NAME'
            )
        return self.task.METHOD_NAME

    @property
    def _single_threaded(self):
        return bool(
            self.task.args.single_threaded or
            dbt.flags.SINGLE_THREADED_HANDLER
        )

    @property
    def timeout(self) -> Optional[float]:
        if self.task_params is None or self.task_params.timeout is None:
            return None
        # task_params.timeout is a `Real` for encoding reasons, but we just
        # want it as a float.
        return float(self.task_params.timeout)

    @property
    def tags(self) -> Optional[TaskTags]:
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
            raise InternalException(
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
                f'Invalid message type {msg.message_type} ({msg})'
            )

    def get_result(self) -> RemoteResult:
        if self.process is None:
            raise InternalException(
                'get_result() called before handle()'
            )

        flags = self.task.get_flags()

        # If we blocked the manifest tasks, we need to un-set them on exit.
        # threaded mode handles this on its own.
        with get_results_context(flags, self.manager, lambda: self.logs):
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
                exc.logs = [log.to_dict() for log in self.logs]
                exc.tags = self.tags
                raise

            # results get real logs
            result.logs = self.logs[:]
            return result

    def run(self):
        try:
            with StateHandler(self):
                self.result = self.get_result()
        except (dbt.exceptions.Exception, RPCException):
            # we probably got an error after the RPC call ran (and it was
            # probably deps...). By now anyone who wanted to see it has seen it
            # so we can suppress it to avoid stderr stack traces
            pass

    def handle_singlethreaded(
        self, kwargs: Dict[str, Any], flags: RemoteMethodFlags
    ):
        # in single-threaded mode, we're going to remain synchronous, so call
        # `run`, not `start`, and return an actual result.
        # note this shouldn't call self.run() as that has different semantics
        # (we want errors to raise)
        if self.process is None:  # mypy appeasement
            raise InternalException(
                'Cannot run a None process'
            )
        self.process.task_exec()
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
        if self.process is None:
            raise InternalException('self.process is None in start()!')
        self.process.start()
        self.state = TaskHandlerState.Running
        super().start()

    def _collect_parameters(self):
        # both get_parameters and the argparse can raise a TypeError.
        cls: Type[RPCParameters] = self.task.get_parameters()

        if self.task_kwargs is None:
            raise TypeError(
                'task_kwargs were None - unable to collect parameters'
            )

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

        with SetArgsStateHandler(self):
            # this will raise a TypeError if you provided bad arguments.
            self.task_params = self._collect_parameters()
            self.task.set_args(self.task_params)
            # now that we have called set_args, we can figure out our flags
            flags: RemoteMethodFlags = self.task.get_flags()
            if RemoteMethodFlags.RequiresConfigReloadBefore in flags:
                # tell the manager to reload the config.
                self.manager.reload_config()
            # set our task config to the version on our manager now. RPCCLi
            # tasks use this to set their `real_task`.
            self.task.set_config(self.manager.config)
            if self.task_params is None:  # mypy appeasement
                raise InternalException(
                    'Task params set to None!'
                )

            if RemoteMethodFlags.Builtin in flags:
                # bypass the queue, logging, etc: Straight to the method
                return self.task.handle_request()

        self.subscriber = QueueSubscriber(dbt.flags.MP_CONTEXT.Queue())
        self.process = BootstrapProcess(self.task, self.subscriber.queue)

        if RemoteMethodFlags.BlocksManifestTasks in flags:
            # got a request to do some compiling, but we already are!
            if not self.manager.set_parsing():
                raise dbt_error(dbt.exceptions.RPCCompiling())

        if self._single_threaded:
            # all requests are synchronous in single-threaded mode. No need to
            # create a process...
            return self.handle_singlethreaded(kwargs, flags)

        self.start()
        return {'request_token': str(self.task_id)}

    def __call__(self, **kwargs: Dict[str, Any]) -> Dict[str, Any]:
        # __call__ happens deep inside jsonrpc's framework
        self.manager.add_request(self)
        return self.handle(kwargs)
