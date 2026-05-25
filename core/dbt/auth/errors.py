from dbt_common.exceptions import DbtBaseException


class AuthError(DbtBaseException):
    pass


class NotAuthenticated(AuthError):
    def __init__(self):
        super().__init__("not authenticated: no credentials found")


class AuthenticationExpired(AuthError):
    def __init__(self):
        super().__init__("authentication expired")


class InaccessibleSource(AuthError):
    def __init__(self, source: str, cause: Exception):
        self.source = source
        self.cause = cause
        super().__init__(f"inaccessible source ({source}): {cause}")


class Malformed(AuthError):
    def __init__(self, detail: str):
        super().__init__(f"malformed config: {detail}")


class InteractiveAuthError(AuthError):
    def __init__(self, detail: str):
        super().__init__(f"interactive auth failed: {detail}")


class AuthAborted(AuthError):
    def __init__(self):
        super().__init__("interactive auth aborted")


class InadequateScopes(AuthError):
    def __init__(self, requested: list[str], cached: list[str]):
        self.requested = requested
        self.cached = cached
        super().__init__(
            f"inadequate scopes: cached session has {cached!r} but {requested!r} are required"
        )


class RefreshFailed(AuthError):
    def __init__(self, detail: str):
        super().__init__(f"token refresh failed: {detail}")
