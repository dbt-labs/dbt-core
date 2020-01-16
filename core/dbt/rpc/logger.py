import logbook
import logbook.queues
from jsonrpc.exceptions import JSONRPCError
from hologram import JsonSchemaMixin
from hologram.helpers import StrEnum

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from queue import Empty
from typing import Optional, Any

from dbt.contracts.rpc import (
    RemoteResult,
)
from dbt.exceptions import InternalException
from dbt.utils import restrict_to


class QueueMessageType(StrEnum):
    Error = 'error'
    Result = 'result'
    Timeout = 'timeout'
    Log = 'log'

    terminating = frozenset((Error, Result, Timeout))


@dataclass
class QueueMessage(JsonSchemaMixin):
    message_type: QueueMessageType


@dataclass
class QueueLogMessage(QueueMessage):
    message_type: QueueMessageType = field(
        metadata=restrict_to(QueueMessageType.Log)
    )
    record: logbook.LogRecord

    @classmethod
    def from_record(cls, record: logbook.LogRecord):
        return QueueLogMessage(
            message_type=QueueMessageType.Log,
            record=record,
        )


@dataclass
class QueueErrorMessage(QueueMessage):
    message_type: QueueMessageType = field(
        metadata=restrict_to(QueueMessageType.Error)
    )
    error: JSONRPCError

    @classmethod
    def from_error(cls, error: JSONRPCError):
        return QueueErrorMessage(
            message_type=QueueMessageType.Error,
            error=error,

        )


@dataclass
class QueueResultMessage(QueueMessage):
    message_type: QueueMessageType = field(
        metadata=restrict_to(QueueMessageType.Result)
    )
    result: RemoteResult

    @classmethod
    def from_result(cls, result: RemoteResult):
        return cls(
            message_type=QueueMessageType.Result,
            result=result,
        )


@dataclass
class QueueTimeoutMessage(QueueMessage):
    message_type: QueueMessageType = field(
        metadata=restrict_to(QueueMessageType.Timeout),
    )

    @classmethod
    def create(cls):
        return cls(message_type=QueueMessageType.Timeout)


class QueueLogHandler(logbook.queues.MultiProcessingHandler):
    def emit(self, record: logbook.LogRecord):
        # trigger the cached proeprties here
        record.pull_information()
        self.queue.put_nowait(QueueLogMessage.from_record(record))

    def emit_error(self, error: JSONRPCError):
        self.queue.put_nowait(QueueErrorMessage.from_error(error))

    def emit_result(self, result: RemoteResult):
        self.queue.put_nowait(QueueResultMessage.from_result(result))


def _next_timeout(
    started: datetime,
    timeout: Optional[float],
) -> Optional[float]:
    if timeout is None:
        return None

    end = started + timedelta(seconds=timeout)
    message_timeout = end - datetime.utcnow()
    return message_timeout.total_seconds()


class QueueSubscriber(logbook.queues.MultiProcessingSubscriber):
    def _recv_raw(self, timeout: Optional[float]) -> Any:
        if timeout is None:
            return self.queue.get()

        if timeout < 0:
            return QueueTimeoutMessage.create()

        try:
            return self.queue.get(block=True, timeout=timeout)
        except Empty:
            return QueueTimeoutMessage.create()

    def recv(
        self,
        timeout: Optional[float] = None
    ) -> QueueMessage:
        """Receives one record from the socket, loads it and dispatches it.
        Returns the message type if something was dispatched or `None` if it
        timed out.
        """
        rv = self._recv_raw(timeout)
        if not isinstance(rv, QueueMessage):
            raise InternalException(
                'Got invalid queue message: {}'.format(rv)
            )
        return rv

    def handle_message(
        self,
        timeout: Optional[float]
    ) -> QueueMessage:
        msg = self.recv(timeout)
        if isinstance(msg, QueueLogMessage):
            logbook.dispatch_record(msg.record)
            return msg
        elif msg.message_type in QueueMessageType.terminating:
            return msg
        else:
            raise InternalException(
                'Got invalid queue message type {}'.format(msg.message_type)
            )

    def dispatch_until_exit(
        self,
        started: datetime,
        timeout: Optional[float] = None
    ) -> QueueMessage:
        while True:
            message_timeout = _next_timeout(started, timeout)
            msg = self.handle_message(message_timeout)
            if msg.message_type in QueueMessageType.terminating:
                return msg


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
