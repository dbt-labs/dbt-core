from jsonrpc.exceptions import JSONRPCDispatchException, JSONRPCInvalidParams

import dbt.exceptions


class RPCException(JSONRPCDispatchException):
    def __init__(self, code=None, message=None, data=None, logs=None):
        if code is None:
            code = -32000
        if message is None:
            message = 'Server error'
        if data is None:
            data = {}

        super(RPCException, self).__init__(code=code, message=message,
                                           data=data)
        self.logs = logs

    @property
    def logs(self):
        return self.error.data.get('logs')

    @logs.setter
    def logs(self, value):
        if value is None:
            return
        self.error.data['logs'] = value

    @classmethod
    def from_error(cls, err):
        return cls(err.code, err.message, err.data, err.data.get('logs'))


def invalid_params(err, logs):
    return RPCException(
        code=JSONRPCInvalidParams.code,
        message=JSONRPCInvalidParams.MESSAGE,
        data={'logs': logs}
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


class QueueMessageType(object):
    Error = 'error'
    Result = 'result'
    Log = 'log'

    @classmethod
    def terminating(cls):
        return [
            cls.Error,
            cls.Result
        ]
