import json

from hologram.helpers import StrEnum
from jsonrpc.exceptions import (
    JSONRPCParseError,
    JSONRPCInvalidRequestException,
    JSONRPCInvalidRequest,
)
from jsonrpc import JSONRPCResponseManager
from jsonrpc.jsonrpc import JSONRPCRequest
from jsonrpc.jsonrpc2 import JSONRPC20Response

import dbt.exceptions
import dbt.tracking
from dbt.logger import GLOBAL_LOGGER as logger
from dbt.rpc.logger import RequestContext
from dbt.rpc.task_handler import RequestTaskHandler
from dbt.rpc.task import RemoteCallable


class TaskHandlerState(StrEnum):
    NotStarted = 'not started'
    Initializing = 'initializing'
    Running = 'running'
    Finished = 'finished'


def track_rpc_request(task):
    dbt.tracking.track_rpc_request({
        "task": task
    })


SYNCHRONOUS_REQUESTS = False


class RequestDispatcher:
    """A special dispatcher that knows about requests."""
    def __init__(self, http_request, json_rpc_request, manager):
        self.http_request = http_request
        self.json_rpc_request = json_rpc_request
        self.manager = manager

    def __getitem__(self, key):
        handler = self.manager.get_handler(
            key,
            self.http_request,
            self.json_rpc_request,
        )
        if handler is None:
            raise KeyError(key)
        if isinstance(handler, RemoteCallable):
            # the handler must be a task. Wrap it.
            return RequestTaskHandler(
                self.manager, handler, self.http_request, self.json_rpc_request
            )
        else:
            return handler


class ResponseManager(JSONRPCResponseManager):
    """Override the default response manager to handle request metadata and
    track in-flight tasks via the task manager.
    """
    @classmethod
    def handle_valid_request(cls, http_request, request, task_manager):
        with RequestContext(request):
            logger.info('handling {} request'.format(request.method))
            track_rpc_request(request.method)

            dispatcher = RequestDispatcher(
                http_request, request, task_manager
            )

            return cls.handle_request(request, dispatcher)

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

        return cls.handle_valid_request(
            http_request, request, task_manager
        )
