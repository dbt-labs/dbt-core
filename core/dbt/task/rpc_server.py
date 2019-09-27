import json

from werkzeug.wsgi import DispatcherMiddleware
from werkzeug.wrappers import Request, Response
from werkzeug.serving import run_simple
from werkzeug.exceptions import NotFound

from dbt.logger import RPC_LOGGER as logger
from dbt.task.base import ConfiguredTask
from dbt.task.compile import CompileTask, RemoteCompileTask
from dbt.task.run import RemoteRunTask
from dbt.utils import JSONEncoder
from dbt import rpc


class RPCServerTask(ConfiguredTask):
    def __init__(self, args, config, tasks=None):
        super(RPCServerTask, self).__init__(args, config)
        # compile locally
        self.manifest = self._compile_manifest()
        self.manifest.build_flat_graph()
        self.task_manager = rpc.TaskManager()
        tasks = tasks or [RemoteCompileTask, RemoteRunTask]
        for cls in tasks:
            task = cls(args, config, self.manifest)
            self.task_manager.add_task_handler(task)

    def _compile_manifest(self):
        compile_task = CompileTask(self.args, self.config)
        compile_task.run()
        return compile_task.manifest

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
            'Supported methods: {}'.format(self.task_manager.methods())
        )

        logger.info(
            'Send requests to http://{}:{}/jsonrpc'.format(display_host, port)
        )

        app = self.handle_request
        app = DispatcherMiddleware(app, {
            '/jsonrpc': self.handle_jsonrpc_request,
        })

        # we have to run in threaded mode if we want to share subprocess
        # handles, which is the easiest way to implement `kill` (it makes `ps`
        # easier as well). The alternative involves tracking metadata+state in
        # a multiprocessing.Manager, adds polling the manager to the request
        # task handler and in general gets messy fast.
        run_simple(host, port, app, threaded=not self.args.single_threaded)

    @Request.application
    def handle_jsonrpc_request(self, request):
        msg = 'Received request ({0}) from {0.remote_addr}, data={0.data}'
        logger.info(msg.format(request))
        response = rpc.ResponseManager.handle(request, self.task_manager)
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

    @Request.application
    def handle_request(self, request):
        raise NotFound()
