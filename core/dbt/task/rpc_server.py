import json
import os

from jsonrpc import Dispatcher, JSONRPCResponseManager

from werkzeug.wrappers import Request, Response
from werkzeug.serving import run_simple

from dbt.logger import GLOBAL_LOGGER as logger
from dbt.task.base import ConfiguredTask
from dbt.task.compile import CompileTask, RemoteCompileTask
from dbt.task.run import RemoteRunTask
from dbt.utils import JSONEncoder


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
        self.dispatcher.add_method(task.safe_handle_request,
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
        logger.info('Received request ({}), data={}'.format(request,
                                                            request.data))
        # request_data is the request as a parsedjson object
        response = JSONRPCResponseManager.handle(
            request.data, self.dispatcher
        )
        json_data = json.dumps(response.data, cls=JSONEncoder)
        response = Response(json_data, mimetype='application/json')
        # this looks and feels dumb, but our json encoder converts decimals and
        # datetimes, and if we use the json_data itself the output looks silly
        # because of escapes, so re-serialize it into valid JSON types for
        # logging.
        logger.info('sending response ({}), data={}'.format(
            response, json.loads(json_data))
        )
        return response
