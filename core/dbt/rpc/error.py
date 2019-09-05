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

    @classmethod
    def from_error(cls, err):
        return cls(err.code, err.message, err.data, err.data.get('logs'))


def invalid_params(data):
    return RPCException(
        code=JSONRPCInvalidParams.CODE,
        message=JSONRPCInvalidParams.MESSAGE,
        data=data
    )


def server_error(err, logs=None):
    exc = dbt.exceptions.Exception(str(err))
    return dbt_error(exc, logs)


def timeout_error(timeout_value, logs=None):
    exc = dbt.exceptions.RPCTimeoutException(timeout_value)
    return dbt_error(exc, logs)


def dbt_error(exc, logs=None):
    exc = RPCException(code=exc.CODE, message=exc.MESSAGE, data=exc.data(),
                       logs=logs)
    return exc
