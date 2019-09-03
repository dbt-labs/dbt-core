import multiprocessing
import os
import signal
import time
import uuid
from typing import Any, Dict, Union

from hologram.helpers import StrEnum

import dbt.exceptions
from dbt import flags
from dbt.adapters.factory import load_plugin, cleanup_connections
from dbt.logger import GLOBAL_LOGGER as logger, list_handler
from dbt.rpc.error import (
    dbt_error,
    server_error,
    RPCException,
    timeout_error,
)
from dbt.rpc.logger import (
    QueueSubscriber,
    QueueLogHandler,
    QueueMessageType,
)


class TaskHandlerState(StrEnum):
    NotStarted = 'not started'
    Initializing = 'initializing'
    Running = 'running'
    Finished = 'finished'


def sigterm_handler(signum, frame):
    raise dbt.exceptions.RPCKilledException(signum)


def _nt_setup(config, args):
    """
    On windows, we have to do a some things that dbt does dynamically at
    process load.

    These things are inherited automatically on posix, where fork() keeps
    everything in memory.
    """
    # reset flags
    flags.set_from_args(args)
    # reload the active plugin
    load_plugin(config.credentials.type)

    # reset tracking, etc
    config.config.set_values(args.profiles_dir)


def _task_bootstrap(task, queue, kwargs):
    signal.signal(signal.SIGTERM, sigterm_handler)
    # the first thing we do in a new process: push logging back over our queue
    handler = QueueLogHandler(queue)
    handler.push_application()
    # on windows, we need to reload our plugins because of how it starts new
    # processes. At this point there are no adapter plugins loaded!
    if os.name == 'nt':
        _nt_setup(task.config, task.args)

    error = None
    result = None
    try:
        result = task.handle_request(**kwargs).to_dict()
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
        handler.emit_error(error.error)
    else:
        handler.emit_result(result)
    # pop the handler so the queue is released so the underlying thread can
    # join
    handler.pop_application()


class RequestTaskHandler:
    """Handler for the single task triggered by a given jsonrpc request."""
    def __init__(self, manager, task, http_request, json_rpc_request):
        self.manager = manager
        self.task = task
        self.http_request = http_request
        self.json_rpc_request = json_rpc_request
        self.subscriber = None
        self.process = None
        self.started = None
        self.timeout = None
        self.logs = []
        self.task_id: uuid.UUID = uuid.uuid4()

    @property
    def request_source(self) -> str:
        return self.http_request.remote_addr

    @property
    def request_id(self) -> Union[str, int]:
        return self.json_rpc_request._id

    @property
    def method(self) -> str:
        return self.task.METHOD_NAME

    def _wait_for_results(self) -> Dict[str, Any]:
        """Wait for results off the queue. If there is an exception raised,
        raise an appropriate RPC exception.

        This does not handle joining, but does terminate the process if it
        timed out.
        """
        try:
            msgtype, result = self.subscriber.dispatch_until_exit(
                started=self.started,
                timeout=self.timeout,
            )
        except dbt.exceptions.Exception as exc:
            raise dbt_error(exc)
        except Exception as exc:
            raise server_error(exc)
        if msgtype == QueueMessageType.Error:
            raise RPCException.from_error(result)
        elif msgtype == QueueMessageType.Timeout:
            if not self.task.args.single_threaded:
                self.process.terminate()
            raise timeout_error(self.timeout)
        elif msgtype == QueueMessageType.Result:
            return result
        else:
            raise dbt.exceptions.InternalException(
                'Invalid message type {} (result={})'.format(msgtype, result)
            )

    def get_result(self) -> Dict[str, Any]:
        try:
            with list_handler(self.logs):
                try:
                    result = self._wait_for_results()
                finally:
                    if not self.task.args.single_threaded:
                        self.process.join()
        except RPCException as exc:
            exc.logs = [l.to_dict() for l in self.logs]
            raise

        result['logs'] = [l.to_dict() for l in self.logs]
        return result

    def handle(self, kwargs: Dict[str, Any]) -> Dict[str, Any]:
        self.started = time.time()
        self.timeout = kwargs.pop('timeout', None)
        self.subscriber = QueueSubscriber()
        if self.task.args.single_threaded:
            _task_bootstrap(self.task, self.subscriber.queue, kwargs)
            return self.get_result()
        else:
            # this is pretty unfortunate, but we have to reset the adapter
            # cache _before_ we fork on posix. libpq, but also any other
            # adapters that rely on file descriptors, get really messed up if
            # you fork(), because the fds get inherited but the state isn't
            # shared. The child process and the parent might end up trying to
            # do things on the same fd at the same time.
            # Also for some reason, if you do this after forking, even without
            # calling close(), the connection in the parent ends up throwing
            # 'connection already closed' exceptions
            if os.name != 'nt':
                cleanup_connections()
            self.process = multiprocessing.Process(
                target=_task_bootstrap,
                args=(self.task, self.subscriber.queue, kwargs)
            )
            self.process.start()
            result = self.get_result()
        return result

    @property
    def state(self) -> TaskHandlerState:
        if self.started is None:
            return TaskHandlerState.NotStarted
        elif self.process is None:
            return TaskHandlerState.Initializing
        elif self.process.is_alive():
            return TaskHandlerState.Running
        else:
            return TaskHandlerState.Finished

    def __call__(self, **kwargs) -> Dict[str, Any]:
        # __call__ happens deep inside jsonrpc's framework
        try:
            self.manager.add_request(self)
            return self.handle(kwargs)
        finally:
            self.manager.mark_done(self)
