"""Functional tests for the dbt login OAuth flow.

Tests exercise token exchange, session persistence, post-login callbacks,
and the refresh flow. The callback server and browser opener are mocked out.
"""

from __future__ import annotations

import base64
import json
import time
from argparse import Namespace
from unittest import mock
from urllib.parse import parse_qs, parse_qsl, urlencode, urlparse, urlunparse

from dbt.auth.credentials import OAuthSession, PlatformCredential, StateCredential
from dbt.auth.oauth.platform import build_context as build_platform_ctx
from dbt.auth.oauth.platform import exchange_code as platform_exchange_code
from dbt.auth.oauth.platform import on_platform_login_success
from dbt.auth.oauth.platform import resolve_from_callback as platform_resolve
from dbt.auth.oauth.state import build_context as build_state_ctx
from dbt.auth.oauth.state import on_state_login_success
from dbt.auth.oauth.state import resolve_from_callback as state_resolve
from dbt.auth.resolvers import OAuthPassiveResolver
from dbt.auth.session_cache import read_session_cache, upsert_session


def _make_jwt(
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


FAKE_JWT = _make_jwt()
FAKE_ACCOUNT_URL = "https://ab123.us1.dbt.com"


def _platform_token_response(**overrides) -> dict:
    defaults = {
        "access_token": FAKE_JWT,
        "refresh_token": "refresh_tok",
        "id_token": "id_tok",
        "scope": "user_access offline_access",
        "expires_in": 3600,
    }
    defaults.update(overrides)
    return defaults


def _state_token_response(**overrides) -> dict:
    defaults = {
        "access_token": "rc_access_tok",
        "refresh_token": "rc_refresh_tok",
        "id_token": "rc_id_tok",
        "scope": "runcache:scope:orgs",
        "expires_in": 900,
    }
    defaults.update(overrides)
    return defaults


def _mock_post(token_response):
    resp = mock.Mock()
    resp.ok = True
    resp.status_code = 200
    resp.json.return_value = token_response
    return resp


def _mock_get(body=None):
    resp = mock.Mock()
    resp.ok = True
    resp.status_code = 200
    resp.raise_for_status = mock.Mock()
    resp.json.return_value = body or {}
    resp.text = json.dumps(body or {})
    return resp


class TestPlatformLoginFlow:
    """Token exchange → session persistence → JWKS fetch."""

    def test_resolve_from_callback_exchanges_and_persists(self, tmp_path):
        cache_path = tmp_path / "oauth_sessions.json"
        token_resp = _platform_token_response()
        callback_result = {"code": "auth_code_123", "account_url": FAKE_ACCOUNT_URL}

        with mock.patch(
            "dbt.auth.oauth.platform.requests.post", return_value=_mock_post(token_resp)
        ), mock.patch(
            "dbt.auth.oauth.platform.requests.get", return_value=_mock_get({"keys": []})
        ):
            cred = platform_resolve(
                result=callback_result,
                client_id="test_client",
                pkce_verifier="test_verifier",
                redirect_url="http://localhost:12345/",
                cache_path=cache_path,
            )

        assert isinstance(cred, PlatformCredential)
        assert cred.account_id == 42
        assert cred.account_host == "ab123.us1.dbt.com"
        assert cred.token == FAKE_JWT

        cache = read_session_cache(cache_path)
        assert len(cache.sessions) == 1
        assert cache.sessions[0].access_token == FAKE_JWT
        assert cache.sessions[0].client_id == "test_client"
        assert cache.sessions[0].refresh_token == "refresh_tok"

    def test_exchange_sends_correct_form_data(self):
        token_resp = _platform_token_response()
        with mock.patch(
            "dbt.auth.oauth.platform.requests.post", return_value=_mock_post(token_resp)
        ) as mock_post:
            platform_exchange_code(
                account_url="https://ab123.us1.dbt.com",
                client_id="my_client",
                code="the_code",
                redirect_url="http://localhost:9999/",
                code_verifier="the_verifier",
            )

        call_kwargs = mock_post.call_args
        assert call_kwargs[1]["data"]["grant_type"] == "authorization_code"
        assert call_kwargs[1]["data"]["code"] == "the_code"
        assert call_kwargs[1]["data"]["client_id"] == "my_client"
        assert call_kwargs[1]["data"]["code_verifier"] == "the_verifier"
        assert "account_url=https" in call_kwargs[1]["data"]["redirect_uri"]


class TestPostPlatformLogin:
    """Post-login decision matrix: get_flags().MANAGE_STATE for resolved check,
    get_user_setting_flag() to detect explicit disable, set_user_setting_flag to write."""

    def _run_post_login(
        self,
        configured: bool,
        enabled: bool,
        user_setting: object = None,
        confirm: bool = True,
    ):
        """Run on_platform_login_success.

        enabled: resolved MANAGE_STATE from get_flags() (CLI > env > project > user_settings)
        user_setting: explicit value in user_settings.yml (True/False/None=not set)
        """
        cred = PlatformCredential(
            token=FAKE_JWT,
            expires_at=time.time() + 3600,
            account_host="ab123.us1.dbt.com",
            account_id=42,
        )
        flags = Namespace(MANAGE_STATE=enabled)
        fired_messages = []

        def capture_event(event):
            if hasattr(event, "msg"):
                fired_messages.append(event.msg)

        with mock.patch("dbt.flags.get_flags", return_value=flags), mock.patch(
            "dbt.auth.oauth.platform.get_user_setting_flag", return_value=user_setting
        ), mock.patch(
            "dbt.auth.oauth.platform.DbtPlatformAPIClient.is_state_configured",
            return_value=configured,
        ), mock.patch(
            "dbt.auth.oauth.platform.DbtPlatformAPIClient.warm_license_cache"
        ), mock.patch(
            "dbt.auth.oauth.platform.fire_event", side_effect=capture_event
        ), mock.patch(
            "dbt.auth.oauth.platform.set_user_setting_flag"
        ) as mock_set_flag, mock.patch(
            "dbt.auth.oauth.platform.click.confirm", return_value=confirm
        ):
            on_platform_login_success(cred)

        return mock_set_flag, fired_messages

    def test_already_enabled_and_configured_is_noop(self):
        mock_set_flag, messages = self._run_post_login(
            configured=True, enabled=True, user_setting=True
        )
        mock_set_flag.assert_not_called()
        assert any("Congratulations" in m for m in messages)

    def test_already_enabled_but_not_configured_warns(self):
        mock_set_flag, messages = self._run_post_login(
            configured=False, enabled=True, user_setting=True
        )
        mock_set_flag.assert_not_called()
        assert any("not in your dbt platform" in m for m in messages)

    def test_enabled_via_env_overrides_user_settings_false(self):
        """MANAGE_STATE=True from env/CLI even though user_settings has false."""
        mock_set_flag, messages = self._run_post_login(
            configured=True, enabled=True, user_setting=False
        )
        mock_set_flag.assert_not_called()

    def test_explicitly_disabled_user_confirms(self):
        mock_set_flag, messages = self._run_post_login(
            configured=True, enabled=False, user_setting=False, confirm=True
        )
        mock_set_flag.assert_called_once_with("manage_state", True)
        assert any("Configuration written" in m for m in messages)

    def test_explicitly_disabled_user_declines(self):
        mock_set_flag, messages = self._run_post_login(
            configured=True, enabled=False, user_setting=False, confirm=False
        )
        mock_set_flag.assert_not_called()
        assert any("you can modify ~/.dbt/user_settings.yml" in m for m in messages)

    def test_explicitly_disabled_not_configured_user_confirms(self):
        mock_set_flag, messages = self._run_post_login(
            configured=False, enabled=False, user_setting=False, confirm=True
        )
        mock_set_flag.assert_called_once_with("manage_state", True)
        assert any("not in your dbt platform" in m for m in messages)

    def test_not_set_auto_enables(self):
        mock_set_flag, messages = self._run_post_login(
            configured=True, enabled=False, user_setting=None
        )
        mock_set_flag.assert_called_once_with("manage_state", True)
        assert any("Configuration written" in m for m in messages)

    def test_not_set_not_configured_auto_enables_and_warns(self):
        mock_set_flag, messages = self._run_post_login(
            configured=False, enabled=False, user_setting=None
        )
        mock_set_flag.assert_called_once_with("manage_state", True)
        assert any("not in your dbt platform" in m for m in messages)


class TestTokenRefreshFlow:
    """OAuthPassiveResolver refreshes an expired token and persists the new session."""

    def test_refresh_expired_token_persists_new_session(self, tmp_path):
        cache_path = tmp_path / "oauth_sessions.json"
        expired_session = OAuthSession(
            access_token="old_expired_jwt",
            scopes=["user_access", "offline_access"],
            expires_at=time.time() - 3600,
            account_host="ab123.us1.dbt.com",
            account_id=42,
            user_id=7,
            client_id="test_client",
            refresh_token="old_refresh_tok",
        )
        upsert_session(expired_session, cache_path)

        new_jwt = _make_jwt()
        refresh_resp = _mock_post(
            {
                "access_token": new_jwt,
                "refresh_token": "new_refresh_tok",
                "scope": "user_access offline_access",
                "expires_in": 3600,
            }
        )

        resolver = OAuthPassiveResolver("test_client", cache_path=cache_path)

        with mock.patch("dbt.auth.resolvers.requests.post", return_value=refresh_resp):
            cred = resolver.resolve()

        assert isinstance(cred, PlatformCredential)
        assert cred.token == new_jwt
        assert cred.account_id == 42

        cache = read_session_cache(cache_path)
        assert len(cache.sessions) == 1
        assert cache.sessions[0].access_token == new_jwt
        assert cache.sessions[0].refresh_token == "new_refresh_tok"

    def test_refresh_sends_scope_in_request(self, tmp_path):
        cache_path = tmp_path / "oauth_sessions.json"
        upsert_session(
            OAuthSession(
                access_token="old",
                scopes=["user_access", "offline_access"],
                expires_at=time.time() - 100,
                account_host="ab123.us1.dbt.com",
                account_id=42,
                user_id=7,
                client_id="test_client",
                refresh_token="ref_tok",
            ),
            cache_path,
        )

        new_jwt = _make_jwt()
        refresh_resp = _mock_post(
            {
                "access_token": new_jwt,
                "refresh_token": "new_ref",
                "scope": "user_access offline_access",
                "expires_in": 3600,
            }
        )

        resolver = OAuthPassiveResolver("test_client", cache_path=cache_path)

        with mock.patch(
            "dbt.auth.resolvers.requests.post", return_value=refresh_resp
        ) as mock_post:
            resolver.resolve()

        call_data = mock_post.call_args[1]["data"]
        assert call_data["scope"] == "user_access offline_access"
        assert call_data["grant_type"] == "refresh_token"


class TestStateLoginFlow:
    """State token exchange → state_auth.json persistence → post-login callback."""

    def test_resolve_from_callback_exchanges_and_persists(self):
        token_resp = _state_token_response()
        callback_result = {"dbt_state_oauth": "state_code_123"}
        ctx = {
            "token_url": "https://auth.runcache.com/token",
            "client_id": "rc_client",
            "redirect_uri": "http://localhost:12345/",
            "code_verifier": "rc_verifier",
            "state": "rc_state_abc",
        }

        with mock.patch(
            "dbt.auth.oauth.state.requests.post", return_value=_mock_post(token_resp)
        ), mock.patch("dbt.auth.oauth.state.write_state_auth") as mock_write:
            cred = state_resolve(callback_result, ctx)

        assert isinstance(cred, StateCredential)
        assert cred.token == "rc_access_tok"
        assert cred.refresh_token == "rc_refresh_tok"
        assert cred.scopes == ["runcache:scope:orgs"]

        mock_write.assert_called_once()
        persisted_data = mock_write.call_args[0][0]
        assert persisted_data["access_token"] == "rc_access_tok"

    def test_post_login_sets_manage_state(self):
        cred = StateCredential(
            token="rc_tok",
            expires_at=time.time() + 900,
            refresh_token="rc_ref",
            scopes=["runcache:scope:orgs"],
        )

        with mock.patch("dbt.auth.oauth.state.set_user_setting_flag") as mock_set:
            on_state_login_success(cred)

        mock_set.assert_called_once_with("manage_state", True)

    def test_exchange_sends_correct_form_data(self):
        token_resp = _state_token_response()
        with mock.patch(
            "dbt.auth.oauth.state.requests.post", return_value=_mock_post(token_resp)
        ) as mock_post:
            from dbt.auth.oauth.state import exchange_code

            exchange_code(
                token_url="https://auth.runcache.com/token",
                client_id="rc_client",
                code="the_code",
                redirect_uri="http://localhost:9999/",
                code_verifier="the_verifier",
                state="the_state",
            )

        call_data = mock_post.call_args[1]["data"]
        assert call_data["grant_type"] == "authorization_code"
        assert call_data["code"] == "the_code"
        assert call_data["client_id"] == "rc_client"
        assert call_data["state"] == "the_state"


class TestStateOAuthUrlRoundTrip:
    """The dbt_state_oauth param carries a base64-encoded state authorize URL
    inside the platform authorize URL. A prior bug caused base64 '=' padding to
    collide with URL query syntax when the domain changed to auth.state.dbt.com.
    These tests prove the nested URL survives the round-trip."""

    def _build_combined_url(self, state_auth_url: str = "https://auth.state.dbt.com"):
        """Replicate the URL construction in OAuthInteractiveResolver.resolve()."""
        redirect_url = "http://localhost:54321/"

        platform_ctx = build_platform_ctx(
            redirect_url=redirect_url,
            client_id="test_client",
            auth_server_url="https://us1.dbt.com/register",
            scopes="identity:read offline_access",
        )

        with mock.patch.dict("os.environ", {"RUN_CACHE_AUTH_URL": state_auth_url}, clear=False):
            state_ctx = build_state_ctx(redirect_url)

        parsed = urlparse(platform_ctx["authorize_url"])
        params = parse_qsl(parsed.query)
        params.append(("dbt_state_oauth", state_ctx["encoded_param"]))
        params.append(("_dbtsrc", "dbt-core"))
        combined_url = urlunparse(parsed._replace(query=urlencode(params)))

        return combined_url, state_ctx

    def _extract_state_url(self, combined_url: str) -> str:
        """Parse dbt_state_oauth from the combined URL and base64-decode it."""
        qs = parse_qs(urlparse(combined_url).query)
        assert "dbt_state_oauth" in qs, "dbt_state_oauth missing from combined URL"
        encoded = qs["dbt_state_oauth"][0]
        return base64.b64decode(encoded).decode()

    def test_state_url_roundtrips_with_padding_domain(self):
        """auth.state.dbt.com produces base64 output with '=' padding.
        Verify the decoded state URL retains all expected query params."""
        combined_url, state_ctx = self._build_combined_url("https://auth.state.dbt.com")
        decoded_url = self._extract_state_url(combined_url)

        decoded_qs = parse_qs(urlparse(decoded_url).query)
        assert decoded_qs["client_id"][0] == state_ctx["client_id"]
        assert decoded_qs["state"][0] == state_ctx["state"]
        assert decoded_qs["redirect_uri"][0] == "http://localhost:54321/"
        assert decoded_qs["scope"][0] == "runcache:scope:orgs"
        assert decoded_qs["response_type"][0] == "code"
        assert "code_challenge" in decoded_qs
        assert decoded_qs["code_challenge_method"][0] == "S256"

    def test_state_url_roundtrips_with_no_padding_domain(self):
        """auth.runcache.com (no padding) should also round-trip cleanly."""
        combined_url, state_ctx = self._build_combined_url("https://auth.runcache.com")
        decoded_url = self._extract_state_url(combined_url)

        decoded_qs = parse_qs(urlparse(decoded_url).query)
        assert decoded_qs["client_id"][0] == state_ctx["client_id"]
        assert decoded_qs["state"][0] == state_ctx["state"]
        assert decoded_qs["redirect_uri"][0] == "http://localhost:54321/"

    def test_encoded_param_contains_padding(self):
        """Confirm that auth.state.dbt.com actually produces '=' padding,
        which was the root cause of the original bug."""
        _, state_ctx = self._build_combined_url("https://auth.state.dbt.com")
        encoded = state_ctx["encoded_param"]
        assert (
            "=" in encoded
        ), f"Expected base64 padding in encoded_param for auth.state.dbt.com, got: {encoded}"
