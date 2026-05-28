from __future__ import annotations

import base64
import json
from unittest import mock

import pytest

from dbt.auth.credentials import OAuthSession, PlatformCredential
from dbt.auth.session_cache import upsert_session


def make_jwt(
    user_id: int = 7,
    account_id: int = 42,
    account_host: str = "ab123.us1.dbt.com",
) -> str:
    header = base64.urlsafe_b64encode(b'{"alg":"RS256"}').rstrip(b"=").decode()
    payload = json.dumps(
        {
            "sub": str(user_id),
            "https://dbt.com/account_id": str(account_id),
            "iss": f"https://{account_host}",
        }
    )
    body = base64.urlsafe_b64encode(payload.encode()).rstrip(b"=").decode()
    return f"{header}.{body}.fakesig"


FAKE_JWT = make_jwt()
FAKE_ACCOUNT_URL = "https://ab123.us1.dbt.com"


@pytest.fixture
def fake_jwt():
    return FAKE_JWT


class StubCallbackServer:
    """Stand-in for OAuthCallbackServer that returns a canned result
    without binding a port or blocking on HTTP."""

    def __init__(self, flow: str = "platform"):
        self.server_address = ("localhost", 54321)
        self.platform_oauth_state: str = ""
        self.state_oauth_state: str = ""
        self.result: dict | None = None
        self.error: str | None = None
        self.timeout: int = 600
        self._flow = flow

    def handle_request(self):
        if self._flow == "error":
            self.error = "authentication timed out"
        elif self._flow == "platform":
            self.result = {
                "code": "auth_code_123",
                "account_url": FAKE_ACCOUNT_URL,
            }
        elif self._flow == "state":
            self.result = {"dbt_state_oauth": "state_code_456"}

    def server_close(self):
        pass


class StubPlatformAPIClient:
    """Stand-in for DbtPlatformAPIClient that avoids all network calls."""

    _state_configured: bool = False

    def __init__(self, credential: PlatformCredential) -> None:
        self.credential = credential

    def is_state_configured(self) -> bool:
        return self._state_configured

    def warm_license_cache(self) -> None:
        pass

    def fetch_and_persist_jwks(self) -> None:
        pass


@pytest.fixture
def redirect_cache_paths(tmp_path):
    """Redirect all auth file I/O to tmp_path instead of ~/.dbt/.

    Patches DEFAULT_CACHE_PATH in every module that imports it, plus
    write_state_auth and user-settings paths.  This lets the real
    write_state_auth / upsert_session / set_user_setting_flag run
    against temp files.
    """
    oauth_path = tmp_path / "oauth_sessions.json"
    state_path = tmp_path / "state_auth.json"
    settings_path = tmp_path / "user_settings.yml"

    from dbt.auth import session_cache

    orig_write = session_cache.write_state_auth
    orig_read = session_cache.read_state_auth

    def _write_state(token_data, path=state_path):
        return orig_write(token_data, path)

    def _read_state(path=state_path):
        return orig_read(path)

    with mock.patch("dbt.auth.resolvers.DEFAULT_CACHE_PATH", oauth_path), mock.patch(
        "dbt.auth.session_cache.DEFAULT_CACHE_PATH", oauth_path
    ), mock.patch("dbt.auth.oauth.platform.DEFAULT_CACHE_PATH", oauth_path), mock.patch(
        "dbt.auth.session_cache.STATE_AUTH_PATH", state_path
    ), mock.patch(
        "dbt.auth.session_cache.write_state_auth", _write_state
    ), mock.patch(
        "dbt.auth.session_cache.read_state_auth", _read_state
    ), mock.patch(
        "dbt.task.login.read_state_auth", _read_state
    ), mock.patch(
        "dbt.auth.oauth.state.write_state_auth", _write_state
    ), mock.patch(
        "dbt.config.user_settings._default_path", return_value=settings_path
    ):
        yield oauth_path


@pytest.fixture
def seeded_oauth_cache(redirect_cache_paths):
    """Write a valid, non-expired OAuth session into the temp cache."""
    import time

    session = OAuthSession(
        access_token=FAKE_JWT,
        scopes=["account:read", "identity:read", "offline_access"],
        expires_at=time.time() + 7200,
        account_host="ab123.us1.dbt.com",
        account_id=42,
        user_id=7,
        client_id="854ad54c885f03bbe6ca7eb1e75593fb",
    )
    upsert_session(session, redirect_cache_paths)
    return redirect_cache_paths


@pytest.fixture
def seeded_state_auth(redirect_cache_paths, tmp_path):
    """Write a valid state_auth.json into the temp cache."""
    from dbt.auth.session_cache import write_state_auth

    token_data = {
        "access_token": "state_access_tok",
        "refresh_token": "state_refresh_tok",
        "id_token": "state_id_tok",
        "scope": "runcache:scope:orgs",
        "expires_in": 900,
    }
    write_state_auth(token_data)
    return tmp_path / "state_auth.json"


@pytest.fixture
def stub_callback_server(request):
    """Patch OAuthCallbackServer with a stub for the given flow.

    Default flow is "platform".  Use
    ``@pytest.mark.parametrize("stub_callback_server", ["state"], indirect=True)``
    to override.
    """
    flow = getattr(request, "param", "platform")
    with mock.patch(
        "dbt.auth.resolvers.OAuthCallbackServer",
        return_value=StubCallbackServer(flow),
    ):
        yield


@pytest.fixture
def stub_token_exchange(request):
    """Patch the token exchange HTTP call for the given flow.

    ``request.param`` should be ``{"flow": "platform"|"state", "token_data": {...}}``.
    """
    _MODULE = {
        "platform": "dbt.auth.oauth.platform.requests.post",
        "state": "dbt.auth.oauth.state.requests.post",
    }
    param = getattr(request, "param", None)
    if param is None:
        yield
        return

    resp = mock.Mock()
    resp.ok = True
    resp.status_code = 200
    resp.raise_for_status = mock.Mock()
    resp.json.return_value = param["token_data"]
    resp.text = json.dumps(param["token_data"])

    with mock.patch(_MODULE[param["flow"]], return_value=resp):
        yield


@pytest.fixture
def no_browser():
    with mock.patch("dbt.auth.resolvers.webbrowser.open") as m:
        yield m


@pytest.fixture
def stub_confirm(request):
    """Patch click.confirm in the platform OAuth module.

    Use ``@pytest.mark.parametrize("stub_confirm", [True/False], indirect=True)``
    to control the return value.
    """
    with mock.patch(
        "dbt.auth.oauth.platform.click.confirm",
        return_value=getattr(request, "param", True),
    ) as m:
        yield m


@pytest.fixture
def stub_platform_api(request):
    """Replace DbtPlatformAPIClient with a no-network stub.

    Use ``@pytest.mark.parametrize("stub_platform_api", [True], indirect=True)``
    to set ``state_configured=True``.
    """
    StubPlatformAPIClient._state_configured = getattr(request, "param", False)
    with mock.patch("dbt.auth.oauth.platform.DbtPlatformAPIClient", StubPlatformAPIClient):
        yield


@pytest.fixture
def mock_flags(request):
    """Patch get_flags with configurable MANAGE_STATE.

    Use ``@pytest.mark.parametrize("mock_flags", [True], indirect=True)``
    to set ``manage_state=True``.
    """
    from argparse import Namespace

    flags = Namespace(MANAGE_STATE=getattr(request, "param", False))
    with mock.patch("dbt.flags.get_flags", return_value=flags):
        yield


@pytest.fixture
def seeded_cloud_yaml(redirect_cache_paths, tmp_path):
    """Write a valid dbt_cloud.yml and patch CloudYamlResolver to use it."""
    import yaml

    cloud_yaml = tmp_path / "dbt_cloud.yml"
    cloud_yaml.write_text(
        yaml.dump(
            {
                "version": "1",
                "context": {
                    "active-project": "proj-1",
                    "active-host": "ab123.us1.dbt.com",
                },
                "projects": [
                    {
                        "project-name": "My Project",
                        "project-id": "proj-1",
                        "account-name": "acme",
                        "account-id": "42",
                        "account-host": "ab123.us1.dbt.com",
                        "token-name": "my-token",
                        "token-value": "dbtc_abc123",
                    },
                ],
            }
        )
    )
    with mock.patch(
        "dbt.auth.resolvers.CloudYamlResolver._default_path",
        return_value=cloud_yaml,
    ):
        yield cloud_yaml


@pytest.fixture(autouse=True)
def reset_global_flags():
    """Reset global flags so run_dbt doesn't inject stale --profiles-dir / --project-dir."""
    import dbt.flags

    original = dbt.flags.GLOBAL_FLAGS
    dbt.flags.GLOBAL_FLAGS = None  # type: ignore
    yield
    dbt.flags.GLOBAL_FLAGS = original
