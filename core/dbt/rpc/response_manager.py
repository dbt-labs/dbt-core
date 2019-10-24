import json
from typing import Callable, Dict, Any

from hologram import JsonSchemaMixin
from jsonrpc.exceptions import (
    JSONRPCParseError,
    JSONRPCInvalidRequestException,
    JSONRPCInvalidRequest,
)
from jsonrpc import JSONRPCResponseManager
from jsonrpc.jsonrpc import JSONRPCRequest
from jsonrpc.jsonrpc2 import JSONRPC20Request, JSONRPC20Response
from werkzeug import Request as HTTPRequest

import dbt.exceptions
import dbt.tracking
from dbt.logger import GLOBAL_LOGGER as logger
from dbt.rpc.logger import RequestContext
from dbt.rpc.task_handler import RequestTaskHandler
from dbt.rpc.method import RemoteMethod
from dbt.rpc.task_manager import TaskManager


def track_rpc_request(task):
    dbt.tracking.track_rpc_request({
        "task": task
    })


SYNCHRONOUS_REQUESTS = False


class RequestDispatcher(Dict[str, Callable[..., Dict[str, Any]]]):
    """A special dispatcher that knows about requests."""
    def __init__(
        self,
        http_request: HTTPRequest,
        json_rpc_request: JSONRPC20Request,
        manager: TaskManager,
    ):
        self.http_request = http_request
        self.json_rpc_request = json_rpc_request
        self.manager = manager

    def __getitem__(self, key) -> Callable[..., Dict[str, Any]]:
        handler = self.manager.get_handler(
            key,
            self.http_request,
            self.json_rpc_request,
        )
        if handler is None:
            raise KeyError(key)
        if callable(handler):
            # either an error or a builtin
            return handler
        elif isinstance(handler, RemoteMethod):
            # the handler must be a task. Wrap it in a task handler so it can
            # go async
            return RequestTaskHandler(
                self.manager, handler, self.http_request, self.json_rpc_request
            )
        else:
            raise dbt.exceptions.InternalException(
                f'Got an invalid handler from get_handler. Expected None, '
                f'callable, or RemoteMethod, got {handler}'
            )


class ResponseManager(JSONRPCResponseManager):
    """Override the default response manager to handle request metadata and
    track in-flight tasks via the task manager.
    """
    @classmethod
    def handle_valid_request(
        cls,
        http_request: HTTPRequest,
        request: JSONRPC20Request,
        task_manager: TaskManager,
    ) -> JSONRPC20Response:
        with RequestContext(request):
            logger.info('handling {} request'.format(request.method))
            track_rpc_request(request.method)

            dispatcher = RequestDispatcher(
                http_request, request, task_manager
            )

            return cls.handle_request(request, dispatcher)

    @classmethod
    def _get_responses(cls, requests, dispatcher):
        for output in super()._get_responses(requests, dispatcher):
            # if it's a result, check if it's a JsonSchemaMixin and if so call
            # to_dict
            if hasattr(output, 'result'):
                if isinstance(output.result, JsonSchemaMixin):
                    output.result = output.result.to_dict(omit_none=False)
            yield output

    @classmethod
    def handle(
        cls,
        http_request: HTTPRequest,
        task_manager: TaskManager,
    ) -> JSONRPC20Response:
        request_str: str
        if isinstance(http_request.data, bytes):
            request_str = http_request.data.decode("utf-8")
        else:
            request_str = http_request.data

        try:
            data = json.loads(request_str)
        except (TypeError, ValueError):
            return JSONRPC20Response(error=dict(
                code=JSONRPCParseError.CODE,
                message=JSONRPCParseError.MESSAGE,
            ))

        if data.get('jsonrpc', None) != '2.0':
            return JSONRPC20Response(error=dict(
                code=JSONRPCInvalidRequest.CODE,
                message=JSONRPCInvalidRequest.MESSAGE,
            ))

        try:
            request = JSONRPCRequest.from_data(data)
        except (ValueError, JSONRPCInvalidRequestException):
            return JSONRPC20Response(error=dict(
                code=JSONRPCInvalidRequest.CODE,
                message=JSONRPCInvalidRequest.MESSAGE,
            ))

        if not isinstance(request, JSONRPC20Request):
            return JSONRPC20Response(error=dict(
                code=JSONRPCInvalidRequest.CODE,
                message=JSONRPCInvalidRequest.MESSAGE,
            ))

        result = cls.handle_valid_request(
            http_request, request, task_manager
        )

        return result
