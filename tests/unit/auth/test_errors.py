from dbt.auth.errors import (
    AuthAborted,
    AuthenticationExpired,
    AuthError,
    InaccessibleSource,
    InadequateScopes,
    InteractiveAuthError,
    Malformed,
    NotAuthenticated,
    RefreshFailed,
)
from dbt_common.exceptions import DbtBaseException


class TestErrorHierarchy:
    def test_all_errors_inherit_from_auth_error(self):
        errors = [
            NotAuthenticated(),
            AuthenticationExpired(),
            InaccessibleSource("file.json", OSError("gone")),
            Malformed("bad data"),
            InteractiveAuthError("browser failed"),
            AuthAborted(),
            InadequateScopes(["a"], ["b"]),
            RefreshFailed("timeout"),
        ]
        for err in errors:
            assert isinstance(err, AuthError)

    def test_auth_error_inherits_from_dbt_base(self):
        assert issubclass(AuthError, DbtBaseException)


class TestNotAuthenticated:
    def test_message(self):
        err = NotAuthenticated()
        assert "no credentials found" in str(err)


class TestAuthenticationExpired:
    def test_message(self):
        err = AuthenticationExpired()
        assert "expired" in str(err)


class TestInaccessibleSource:
    def test_message_includes_source_and_cause(self):
        cause = OSError("permission denied")
        err = InaccessibleSource("~/.dbt/oauth_sessions.json", cause)
        assert "oauth_sessions.json" in str(err)
        assert "permission denied" in str(err)

    def test_preserves_attributes(self):
        cause = OSError("gone")
        err = InaccessibleSource("src", cause)
        assert err.source == "src"
        assert err.cause is cause


class TestMalformed:
    def test_message_includes_detail(self):
        err = Malformed("token-value is empty")
        assert "token-value is empty" in str(err)
        assert "malformed config" in str(err)


class TestInteractiveAuthError:
    def test_message(self):
        err = InteractiveAuthError("port 29527 in use")
        assert "port 29527 in use" in str(err)
        assert "interactive auth failed" in str(err)


class TestAuthAborted:
    def test_message(self):
        err = AuthAborted()
        assert "aborted" in str(err)


class TestInadequateScopes:
    def test_message_includes_both_scope_lists(self):
        err = InadequateScopes(
            requested=["account:read", "offline_access"],
            cached=["account:read"],
        )
        assert "offline_access" in str(err)
        assert "account:read" in str(err)

    def test_preserves_attributes(self):
        err = InadequateScopes(requested=["a", "b"], cached=["a"])
        assert err.requested == ["a", "b"]
        assert err.cached == ["a"]


class TestRefreshFailed:
    def test_message(self):
        err = RefreshFailed("connection timeout")
        assert "connection timeout" in str(err)
        assert "token refresh failed" in str(err)
