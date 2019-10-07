import json
import os
import signal
import threading
from contextlib import contextmanager

from werkzeug.wsgi import DispatcherMiddleware
from werkzeug.wrappers import Request, Response
from werkzeug.serving import run_simple
from werkzeug.exceptions import NotFound

from dbt.exceptions import RuntimeException
from dbt.logger import (
    GLOBAL_LOGGER as logger,
    list_handler,
    log_manager,
)
from dbt.task.base import ConfiguredTask
from dbt.task.compile import CompileTask
from dbt.task.remote import RPCTask
from dbt.utils import ForgivingJSONEncoder, env_set_truthy
from dbt import rpc
from dbt.rpc.logger import ServerContext, HTTPRequest, RPCResponse


SINGLE_THREADED_WEBSERVER = env_set_truthy('DBT_SINGLE_THREADED_WEBSERVER')


# SIG_DFL ends up killing the process if multiple build up, but SIG_IGN just
# peacefully carries on
SIG_IGN = signal.SIG_IGN


def reload_manager(task_manager, tasks):
    logs = []
    try:
        compile_task = CompileTask(task_manager.args, task_manager.config)
        with list_handler(logs):
            compile_task.run()
        manifest = compile_task.manifest
        manifest.build_flat_graph()

        for cls in tasks:
            task_manager.add_task_handler(cls, manifest)
    except Exception as exc:
        logs = [r.to_dict() for r in logs]
        task_manager.set_compile_exception(exc, logs=logs)
    else:
        logs = [r.to_dict() for r in logs]
        task_manager.set_ready(logs=logs)


@contextmanager
def signhup_replace():
    """A context manager. Replace the current sighup handler with SIG_IGN on
    entering, and (if the current handler was not SIG_IGN) replace it on
    leaving. This is meant to be used inside a sighup handler itself to
    provide. a sort of locking model.

    This relies on the fact that 1) signals are only handled by the main thread
    (the default in Python) and 2) signal.signal() is "atomic" (only C
    instructions). I'm pretty sure that's reliable on posix.

    This shouldn't replace if the handler wasn't already SIG_IGN, and should
    yield whether it has the lock as its value. Callers shouldn't do
    singal-handling things inside this context manager if it does not have the
    lock (they should just exit the context).
    """
    # Don't use locks here! This is called from inside a signal handler

    # set our handler to ignore signals, capturing the existing one
    current_handler = signal.signal(signal.SIGHUP, SIG_IGN)

    # current_handler should be the handler unless we're already loading a
    # new manifest. So if the current handler is the ignore, there was a
    # double-hup! We should exit and not touch the signal handler, to make
    # sure we let the other signal handler fix it
    is_current_handler = current_handler is not SIG_IGN

    # if we got here, we're the ones in charge of configuring! Yield.
    try:
        yield is_current_handler
    finally:
        if is_current_handler:
            # the signal handler that successfully changed the handler is
            # responsible for resetting, and can't be re-called until it's
            # fixed, so no locking needed

            signal.signal(signal.SIGHUP, current_handler)


class RPCServerTask(ConfiguredTask):
    DEFAULT_LOG_FORMAT = 'json'

    def __init__(self, args, config, tasks=None):
        if os.name == 'nt':
            raise RuntimeException(
                'The dbt RPC server is not supported on windows'
            )
        super().__init__(args, config)
        self._tasks = tasks or self._default_tasks()
        self.task_manager = rpc.TaskManager(self.args, self.config)
        self._reloader = None
        self._reload_task_manager()
        signal.signal(signal.SIGHUP, self._sighup_handler)

    @classmethod
    def pre_init_hook(cls, args):
        """A hook called before the task is initialized."""
        if args.log_format == 'text':
            log_manager.format_text()
        else:
            log_manager.format_json()

    def _reload_task_manager(self):
        """This function can only be running once at a time, as it runs in the
        signal handler we replace
        """
        # mark the task manager invalid for task running
        self.task_manager.set_compiling()
        for task in self._tasks:
            self.task_manager.reserve_handler(task)
        # compile in a thread that will fix up the tag manager when it's done
        reloader = threading.Thread(
            target=reload_manager,
            args=(self.task_manager, self._tasks),
        )
        reloader.start()
        # only assign to _reloader here, to avoid calling join() before start()
        self._reloader = reloader

    def _sighup_handler(self, signum, frame):
        with signhup_replace() as run_task_manger:
            if not run_task_manger:
                # a sighup handler is already active.
                return
            if self._reloader is not None and self._reloader.is_alive():
                # a reloader is already active.
                return
            self._reload_task_manager()

    @staticmethod
    def _default_tasks():
        return RPCTask.recursive_subclasses(named_only=True)

    def single_threaded(self):
        return SINGLE_THREADED_WEBSERVER or self.args.single_threaded

    def run_forever(self):
        host = self.args.host
        port = self.args.port
        addr = (host, port)

        display_host = host
        if host == '0.0.0.0':
            display_host = 'localhost'

        logger.info(
            'Serving RPC server at {}:{}, pid={}'.format(
                *addr, os.getpid()
            )
        )

        logger.info(
            'Supported methods: {}'.format(sorted(self.task_manager.methods()))
        )

        logger.info(
            'Send requests to http://{}:{}/jsonrpc'.format(display_host, port)
        )

        app = self.handle_request
        app = DispatcherMiddleware(app, {
            '/jsonrpc': self.handle_jsonrpc_request,
        })

        # we have to run in threaded mode if we want to share subprocess
        # handles, which is the easiest way to implement `kill` (it makes
        # `ps` easier as well). The alternative involves tracking
        # metadata+state in a multiprocessing.Manager, adds polling the
        # manager to the request  task handler and in general gets messy
        # fast.
        run_simple(host, port, app, threaded=not self.single_threaded)

    def run(self):
        with ServerContext().applicationbound():
            self.run_forever()

    @Request.application
    def handle_jsonrpc_request(self, request):
        with HTTPRequest(request):
            jsonrpc_response = rpc.ResponseManager.handle(
                request, self.task_manager
            )
            json_data = json.dumps(
                jsonrpc_response.data,
                cls=ForgivingJSONEncoder,
            )
            response = Response(json_data, mimetype='application/json')
            # this looks and feels dumb, but our json encoder converts decimals
            # and datetimes, and if we use the json_data itself the output
            # looks silly because of escapes, so re-serialize it into valid
            # JSON types for logging.
            with RPCResponse(jsonrpc_response):
                logger.info('sending response ({}) to {}'.format(
                    response, request.remote_addr)
                )
            return response

    @Request.application
    def handle_request(self, request):
        raise NotFound()
