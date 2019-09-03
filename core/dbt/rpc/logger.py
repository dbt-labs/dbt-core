import logbook
from logbook.queues import MultiProcessingHandler, MultiProcessingSubscriber
from hologram.helpers import StrEnum

import time
from queue import Empty
from typing import Tuple, Optional, Any

from dbt.exceptions import InternalException


class QueueMessageType(StrEnum):
    Error = 'error'
    Result = 'result'
    Timeout = 'timeout'
    Log = 'log'

    terminating = frozenset((Error, Result, Timeout))


class QueueLogHandler(MultiProcessingHandler):
    def emit(self, record):
        self.queue.put_nowait(
            (QueueMessageType.Log, record.to_dict(json_safe=True))
        )

    def emit_error(self, error):
        self.queue.put_nowait((QueueMessageType.Error, error))

    def emit_result(self, result):
        self.queue.put_nowait((QueueMessageType.Result, result))


def _next_timeout(started, timeout):
    if timeout is None:
        return None
    end = started + timeout
    message_timeout = end - time.time()
    return message_timeout


class QueueSubscriber(MultiProcessingSubscriber):
    def _recv_raw(self, timeout: Optional[float]):
        if timeout is None:
            return self.queue.get()

        if timeout < 0:
            return QueueMessageType.Timeout, None

        try:
            return self.queue.get(block=True, timeout=timeout)
        except Empty:
            return QueueMessageType.Timeout, None

    def recv(
        self,
        timeout: Optional[float] = None
    ) -> Tuple[QueueMessageType, Any]:  # mypy: ignore
        """Receives one record from the socket, loads it and dispatches it.
        Returns the message type if something was dispatched or `None` if it
        timed out.
        """
        rv = self._recv_raw(timeout)
        msgtype, data = rv
        if msgtype not in QueueMessageType:
            raise InternalException(
                'Got invalid queue message type {}'.format(msgtype)
            )
        return rv

    def handle_message(
        self,
        timeout: Optional[float]
    ) -> Tuple[QueueMessageType, Any]:
        msgtype, data = self.recv(timeout)
        if msgtype in QueueMessageType.terminating:
            return msgtype, data
        elif msgtype == QueueMessageType.Log:
            record = logbook.LogRecord.from_dict(data)
            logbook.dispatch_record(record)
            # keep watching
            return msgtype, None
        else:
            raise InternalException(
                'Got invalid queue message type {}'.format(msgtype)
            )

    def dispatch_until_exit(
        self,
        started: float,
        timeout: Optional[float] = None
    ) -> Tuple[QueueMessageType, Any]:
        while True:
            message_timeout = _next_timeout(started, timeout)
            msgtype, data = self.handle_message(message_timeout)
            if msgtype in QueueMessageType.terminating:
                return msgtype, data


# a bunch of processors to push/pop that set various rpc-related extras
class ServerContext(logbook.Processor):
    def process(self, record):
        # the server context is the last processor in the stack, so it should
        # not overwrite a context if it's already been set.
        if not record.extra['context']:
            record.extra['context'] = 'server'


class HTTPRequest(logbook.Processor):
    def __init__(self, request):
        self.request = request

    def process(self, record):
        record.extra['addr'] = self.request.remote_addr
        record.extra['http_method'] = self.request.method


class RPCRequest(logbook.Processor):
    def __init__(self, request):
        self.request = request
        super().__init__()

    def process(self, record):
        record.extra['request_id'] = self.request._id
        record.extra['method'] = self.request.method


class RPCResponse(logbook.Processor):
    def __init__(self, response):
        self.response = response
        super().__init__()

    def process(self, record):
        record.extra['response_code'] = 200
        # the request_id could be None if the request was bad
        record.extra['request_id'] = getattr(
            self.response.request, '_id', None
        )


class RequestContext(RPCRequest):
    def process(self, record):
        super().process(record)
        record.extra['context'] = 'request'
