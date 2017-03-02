class Exception(BaseException):
    pass


class RuntimeException(RuntimeError, Exception):
    pass


class ValidationException(RuntimeException):
    pass


class NotImplementedException(Exception):
    pass


class ProgrammingException(Exception):
    pass


class FailedToConnectException(Exception):
    pass
