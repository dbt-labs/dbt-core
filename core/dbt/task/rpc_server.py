import json
import multiprocessing
import time

from jsonrpc import Dispatcher, JSONRPCResponseManager

from werkzeug.wrappers import Request, Response
from werkzeug.serving import run_simple

from dbt.logger import RPC_LOGGER as logger, add_queue_handler
from dbt.task.base import ConfiguredTask
from dbt.task.compile import CompileTask, RemoteCompileTask
from dbt.task.run import RemoteRunTask
from dbt.utils import JSONEncoder
from dbt.compat import QueueEmpty
import dbt.exceptions
from dbt import rpc


class RequestTaskHandler(object):
    def __init__(self, task):
        self.task = task
        self.queue = None
        self.process = None
        self.started = None
        self.timeout = None
        self.logs = []

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

            if msgtype == rpc.QueueMessageType.Log:
                self.logs.append(value)
            elif msgtype in rpc.QueueMessageType.terminating():
                return msgtype, value
            else:
                raise dbt.exceptions.InternalException(
                    'Got invalid queue message type {}'.format(msgtype)
                )

    def _join_process(self):
        try:
            msgtype, result = self._wait_for_results()
        except dbt.exceptions.RPCTimeoutException as exc:
            self.process.terminate()
            raise rpc.timeout_error(self.timeout)
        except dbt.exceptions.Exception as exc:
            raise rpc.dbt_error(exc)
        except Exception as exc:
            raise rpc.server_error(exc)
        finally:
            self.process.join()

        if msgtype == rpc.QueueMessageType.Error:
            raise rpc.RPCException.from_error(result)

        return result

    def get_result(self):
        try:
            result = self._join_process()
        except rpc.RPCException as exc:
            exc.logs = self.logs
            raise

        result['logs'] = self.logs
        return result

    def task_bootstrap(self, kwargs):
        # the first thing we do in a new process: start logging
        add_queue_handler(self.queue)

        error = None
        result = None
        try:
            result = self.task.handle_request(**kwargs)
        except rpc.RPCException as exc:
            error = exc
        except dbt.exceptions.Exception as exc:
            logger.debug('dbt runtime exception', exc_info=True)
            error = rpc.dbt_error(exc)
        except Exception as exc:
            logger.debug('uncaught python exception', exc_info=True)
            error = rpc.server_error(exc)

        # put whatever result we got onto the queue as well.
        if error is not None:
            self.queue.put([rpc.QueueMessageType.Error, error.error])
        else:
            self.queue.put([rpc.QueueMessageType.Result, result])

    def handle(self, kwargs):
        self.started = time.time()
        self.timeout = kwargs.pop('timeout', None)
        self.queue = multiprocessing.Queue()
        self.process = multiprocessing.Process(
            target=self.task_bootstrap,
            args=(kwargs,)
        )
        self.process.start()
        return self.get_result()

    @classmethod
    def factory(cls, task):
        def handler(**kwargs):
            return cls(task).handle(kwargs)
        return handler


class RPCServerTask(ConfiguredTask):
    def __init__(self, args, config, tasks=None):
        super(RPCServerTask, self).__init__(args, config)
        # compile locally
        self.compile_task = CompileTask(args, config)
        self.compile_task.run()
        self.dispatcher = Dispatcher()
        tasks = tasks or [RemoteCompileTask, RemoteRunTask]
        for cls in tasks:
            self.register(cls(args, config))

    def register(self, task):
        self.dispatcher.add_method(RequestTaskHandler.factory(task),
                                   name=task.METHOD_NAME)

    @property
    def manifest(self):
        return self.compile_task.manifest

    def run(self):
        host = self.args.host
        port = self.args.port
        addr = (host, port)

        display_host = host
        if host == '0.0.0.0':
            display_host = 'localhost'

        logger.info(
            'Serving RPC server at {}:{}'.format(*addr)
        )

        logger.info(
            'Supported methods: {}'.format(list(self.dispatcher.keys()))
        )

        logger.info(
            'Send requests to http://{}:{}'.format(display_host, port)
        )

        run_simple(host, port, self.handle_request,
                   processes=self.config.threads)

    @Request.application
    def handle_request(self, request):
        msg = 'Received request ({0}) from {0.remote_addr}, data={0.data}'
        logger.info(msg.format(request))
        response = JSONRPCResponseManager.handle(request.data, self.dispatcher)
        json_data = json.dumps(response.data, cls=JSONEncoder)
        response = Response(json_data, mimetype='application/json')
        # this looks and feels dumb, but our json encoder converts decimals and
        # datetimes, and if we use the json_data itself the output looks silly
        # because of escapes, so re-serialize it into valid JSON types for
        # logging.
        logger.info('sending response ({}) to {}, data={}'.format(
            response, request.remote_addr, json.loads(json_data))
        )
        return response
