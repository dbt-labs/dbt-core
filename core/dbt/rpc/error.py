from typing import List, Dict, Any, Optional

from jsonrpc.exceptions import JSONRPCDispatchException, JSONRPCInvalidParams

import dbt.exceptions


class RPCException(JSONRPCDispatchException):
    def __init__(
        self,
        code: Optional[int] = None,
        message: Optional[str] = None,
        data: Optional[Dict[str, Any]] = None,
        logs: Optional[List[Dict[str, Any]]] = None,
        tags: Optional[Dict[str, Any]] = None
    ) -> None:
        if code is None:
            code = -32000
        if message is None:
            message = 'Server error'
        if data is None:
            data = {}

        super().__init__(code=code, message=message, data=data)
        if logs is not None:
            self.logs = logs
        self.error.data['tags'] = tags

    def __str__(self):
        return (
            'RPCException({0.code}, {0.message}, {0.data}, {1.logs})'
            .format(self.error, self)
        )

    @property
    def logs(self) -> List[Dict[str, Any]]:
        return self.error.data.get('logs')

    @logs.setter
    def logs(self, value):
        if value is None:
            return
        self.error.data['logs'] = value

    @property
    def tags(self):
        return self.error.data.get('tags')

    @tags.setter
    def tags(self, value):
        if value is None:
            return
        self.error.data['tags'] = value

    @classmethod
    def from_error(cls, err):
        return cls(
            code=err.code,
            message=err.message,
            data=err.data,
            logs=err.data.get('logs'),
            tags=err.data.get('tags'),
        )


def invalid_params(data):
    return RPCException(
        code=JSONRPCInvalidParams.CODE,
        message=JSONRPCInvalidParams.MESSAGE,
        data=data
    )


def server_error(err, logs=None, tags=None):
    exc = dbt.exceptions.Exception(str(err))
    return dbt_error(exc, logs, tags)


def timeout_error(timeout_value, logs=None, tags=None):
    exc = dbt.exceptions.RPCTimeoutException(timeout_value)
    return dbt_error(exc, logs, tags)


def dbt_error(exc, logs=None, tags=None):
    exc = RPCException(code=exc.CODE, message=exc.MESSAGE, data=exc.data(),
                       logs=logs, tags=tags)
    return exc
