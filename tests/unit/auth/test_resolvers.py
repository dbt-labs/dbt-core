import base64
import json
import os
import time
from unittest import mock

import pytest

from dbt.auth.credentials import OAuthSession, PlatformCredential
from dbt.auth.oauth.platform import decode_access_token
from dbt.auth.oauth.utils import generate_pkce
from dbt.auth.resolvers import CloudYamlResolver, EnvVarResolver, OAuthPassiveResolver
from dbt.auth.session_cache import upsert_session
from dbt.exceptions import (
    AuthenticationExpired,
    InteractiveAuthError,
    MalformedAuthConfig,
    NotAuthenticated,
    RefreshFailed,
)


def _make_session(**overrides) -> OAuthSession:
    defaults = dict(
        access_token="tok_abc",
        scopes=["account:read"],
        expires_at=time.time() + 3600,
        account_host="ab123.us1.dbt.com",
        account_id=42,
        user_id=7,
        client_id="test_client",
    )
    defaults.update(overrides)
    return OAuthSession(**defaults)


def _make_fake_jwt(
    user_id: int = 7, account_id: int = 42, account_host: str = "ab123.us1.dbt.com"
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


def _valid_cloud_yaml(
    project_id: str = "proj-1",
    host: str = "ab123.us1.dbt.com",
    token: str = "dbtc_abc123",
    account_id: str = "42",
) -> str:
    return f"""
version: "1"
context:
  active-project: "{project_id}"
  active-host: "{host}"
projects:
  - project-name: "My Project"
    project-id: "{project_id}"
    account-name: "acme"
    account-id: "{account_id}"
    account-host: "{host}"
    token-name: "my-token"
    token-value: "{token}"
"""


class TestEnvVarResolver:
    def test_happy_path_service_token(self):
        env = {
            "DBT_CLOUD_ACCOUNT_HOST": "ab123.us1.dbt.com",
            "DBT_CLOUD_TOKEN": "dbtc_abc123",
            "DBT_CLOUD_ACCOUNT_ID": "42",
        }
        with mock.patch.dict(os.environ, env, clear=False):
            cred = EnvVarResolver().resolve()
        assert isinstance(cred, PlatformCredential)
        assert cred.token == "dbtc_abc123"
        assert cred.account_host == "ab123.us1.dbt.com"
        assert cred.account_id == 42

    def test_happy_path_pat(self):
        env = {
            "DBT_CLOUD_ACCOUNT_HOST": "ab123.us1.dbt.com",
            "DBT_CLOUD_TOKEN": "dbtu_user_token",
            "DBT_CLOUD_ACCOUNT_ID": "7",
        }
        with mock.patch.dict(os.environ, env, clear=False):
            cred = EnvVarResolver().resolve()
        assert isinstance(cred, PlatformCredential)

    def test_missing_env_vars_raises_not_authenticated(self):
        env = {}
        with mock.patch.dict(os.environ, env, clear=True):
            with pytest.raises(NotAuthenticated):
                EnvVarResolver().resolve()

    def test_partial_env_vars_raises_not_authenticated(self):
        env = {
            "DBT_CLOUD_ACCOUNT_HOST": "host",
            "DBT_CLOUD_TOKEN": "tok",
            # missing account_id
        }
        with mock.patch.dict(os.environ, env, clear=True):
            with pytest.raises(NotAuthenticated):
                EnvVarResolver().resolve()

    def test_empty_string_env_vars_raises_not_authenticated(self):
        env = {
            "DBT_CLOUD_ACCOUNT_HOST": "",
            "DBT_CLOUD_TOKEN": "tok",
            "DBT_CLOUD_ACCOUNT_ID": "1",
        }
        with mock.patch.dict(os.environ, env, clear=False):
            with pytest.raises(NotAuthenticated):
                EnvVarResolver().resolve()

    def test_non_numeric_account_id_raises_malformed(self):
        env = {
            "DBT_CLOUD_ACCOUNT_HOST": "host",
            "DBT_CLOUD_TOKEN": "tok",
            "DBT_CLOUD_ACCOUNT_ID": "not-a-number",
        }
        with mock.patch.dict(os.environ, env, clear=False):
            with pytest.raises(MalformedAuthConfig, match="not a valid integer"):
                EnvVarResolver().resolve()


class TestOAuthPassiveResolver:
    def test_returns_valid_session(self, tmp_path):
        p = tmp_path / "oauth_sessions.json"
        upsert_session(_make_session(), p)

        cred = OAuthPassiveResolver("test_client", cache_path=p).resolve()
        assert isinstance(cred, PlatformCredential)
        assert cred.token == "tok_abc"
        assert cred.account_id == 42

    def test_missing_cache_raises_not_authenticated(self, tmp_path):
        p = tmp_path / "nonexistent.json"
        with pytest.raises(NotAuthenticated):
            OAuthPassiveResolver("test_client", cache_path=p).resolve()

    def test_empty_cache_raises_not_authenticated(self, tmp_path):
        p = tmp_path / "oauth_sessions.json"
        p.write_text(json.dumps({"version": 1, "sessions": []}))

        with pytest.raises(NotAuthenticated):
            OAuthPassiveResolver("test_client", cache_path=p).resolve()

    def test_no_matching_client_raises_not_authenticated(self, tmp_path):
        p = tmp_path / "oauth_sessions.json"
        upsert_session(_make_session(client_id="other"), p)

        with pytest.raises(NotAuthenticated):
            OAuthPassiveResolver("test_client", cache_path=p).resolve()

    def test_expired_no_refresh_raises_authentication_expired(self, tmp_path):
        p = tmp_path / "oauth_sessions.json"
        upsert_session(_make_session(expires_at=time.time() - 3600, refresh_token=None), p)

        with pytest.raises(AuthenticationExpired):
            OAuthPassiveResolver("test_client", cache_path=p).resolve()

    def test_expired_with_refresh_token_calls_refresh(self, tmp_path):
        p = tmp_path / "oauth_sessions.json"
        upsert_session(
            _make_session(expires_at=time.time() - 3600, refresh_token="old_refresh"), p
        )

        new_jwt = _make_fake_jwt()
        mock_response = mock.Mock()
        mock_response.status_code = 200
        mock_response.ok = True
        mock_response.json.return_value = {
            "access_token": new_jwt,
            "refresh_token": "new_refresh",
            "scope": "account:read offline_access",
            "expires_in": 3600,
        }

        with mock.patch(
            "dbt.auth.resolvers.requests.post", return_value=mock_response
        ) as mock_post:
            cred = OAuthPassiveResolver("test_client", cache_path=p).resolve()

        assert cred.token == new_jwt
        assert cred.account_id == 42

        # Verify scope is forwarded in the refresh request
        call_kwargs = mock_post.call_args
        assert call_kwargs[1]["data"]["scope"] == "account:read"

        # Verify cache was updated
        from dbt.auth.session_cache import read_session_cache

        cache = read_session_cache(p)
        assert cache.sessions[0].refresh_token == "new_refresh"

    def test_refresh_4xx_raises_authentication_expired(self, tmp_path):
        p = tmp_path / "oauth_sessions.json"
        upsert_session(_make_session(expires_at=time.time() - 3600, refresh_token="stale"), p)

        mock_response = mock.Mock()
        mock_response.status_code = 401
        mock_response.ok = False

        with mock.patch("dbt.auth.resolvers.requests.post", return_value=mock_response):
            with pytest.raises(AuthenticationExpired):
                OAuthPassiveResolver("test_client", cache_path=p).resolve()

    def test_refresh_network_error_raises_refresh_failed(self, tmp_path):
        import requests as req

        p = tmp_path / "oauth_sessions.json"
        upsert_session(_make_session(expires_at=time.time() - 3600, refresh_token="tok"), p)

        with mock.patch(
            "dbt.auth.resolvers.requests.post",
            side_effect=req.ConnectionError("refused"),
        ):
            with pytest.raises(RefreshFailed):
                OAuthPassiveResolver("test_client", cache_path=p).resolve()


class TestCloudYamlResolver:
    def test_happy_path_service_token(self, tmp_path):
        p = tmp_path / "dbt_cloud.yml"
        p.write_text(_valid_cloud_yaml())

        cred = CloudYamlResolver(path=p).resolve()
        assert isinstance(cred, PlatformCredential)
        assert cred.token == "dbtc_abc123"
        assert cred.account_host == "ab123.us1.dbt.com"
        assert cred.account_id == 42

    def test_happy_path_pat(self, tmp_path):
        p = tmp_path / "dbt_cloud.yml"
        p.write_text(_valid_cloud_yaml(token="dbtu_user_token"))

        cred = CloudYamlResolver(path=p).resolve()
        assert isinstance(cred, PlatformCredential)

    def test_missing_file_raises_not_authenticated(self, tmp_path):
        p = tmp_path / "nonexistent.yml"
        with pytest.raises(NotAuthenticated):
            CloudYamlResolver(path=p).resolve()

    def test_no_matching_project_raises_not_authenticated(self, tmp_path):
        p = tmp_path / "dbt_cloud.yml"
        p.write_text(_valid_cloud_yaml(project_id="proj-99"))
        # The context says active-project is "proj-99" but we wrote project-id "proj-99"
        # so it should match. Let me make the context mismatch:
        content = """
version: "1"
context:
  active-project: "proj-99"
  active-host: "ab123.us1.dbt.com"
projects:
  - project-name: "My Project"
    project-id: "proj-1"
    account-name: "acme"
    account-id: "42"
    account-host: "ab123.us1.dbt.com"
    token-name: "my-token"
    token-value: "dbtc_abc123"
"""
        p.write_text(content)
        with pytest.raises(NotAuthenticated):
            CloudYamlResolver(path=p).resolve()

    def test_malformed_yaml_raises(self, tmp_path):
        p = tmp_path / "dbt_cloud.yml"
        p.write_text("not: valid: yaml: [[[")

        with pytest.raises(MalformedAuthConfig):
            CloudYamlResolver(path=p).resolve()

    def test_empty_token_value_raises_malformed(self, tmp_path):
        p = tmp_path / "dbt_cloud.yml"
        p.write_text(_valid_cloud_yaml(token=""))

        with pytest.raises(MalformedAuthConfig, match="token-value is empty"):
            CloudYamlResolver(path=p).resolve()

    def test_non_numeric_account_id_raises_malformed(self, tmp_path):
        p = tmp_path / "dbt_cloud.yml"
        p.write_text(_valid_cloud_yaml(account_id="not-a-number"))

        with pytest.raises(MalformedAuthConfig, match="not a valid integer"):
            CloudYamlResolver(path=p).resolve()

    def test_env_var_overrides_active_project(self, tmp_path):
        p = tmp_path / "dbt_cloud.yml"
        content = """
version: "1"
context:
  active-project: "proj-1"
  active-host: "ab123.us1.dbt.com"
projects:
  - project-id: "proj-1"
    project-name: "Default"
    account-name: "acme"
    account-id: "1"
    account-host: "ab123.us1.dbt.com"
    token-name: "tok"
    token-value: "dbtc_default"
  - project-id: "proj-2"
    project-name: "Override"
    account-name: "acme"
    account-id: "2"
    account-host: "ab123.us1.dbt.com"
    token-name: "tok"
    token-value: "dbtc_override"
"""
        p.write_text(content)

        with mock.patch.dict(os.environ, {"DBT_CLOUD_PROJECT_ID": "proj-2"}, clear=False):
            cred = CloudYamlResolver(path=p).resolve()
        assert cred.token == "dbtc_override"
        assert cred.account_id == 2

    def test_env_var_overrides_host(self, tmp_path):
        p = tmp_path / "dbt_cloud.yml"
        content = """
version: "1"
context:
  active-project: "proj-1"
  active-host: "ab123.us1.dbt.com"
projects:
  - project-id: "proj-1"
    project-name: "US"
    account-name: "acme"
    account-id: "1"
    account-host: "ab123.us1.dbt.com"
    token-name: "tok"
    token-value: "dbtc_us"
  - project-id: "proj-1"
    project-name: "EMEA"
    account-name: "acme"
    account-id: "2"
    account-host: "emea.dbt.com"
    token-name: "tok"
    token-value: "dbtc_emea"
"""
        p.write_text(content)

        with mock.patch.dict(os.environ, {"DBT_CLOUD_ACCOUNT_HOST": "emea.dbt.com"}, clear=False):
            cred = CloudYamlResolver(path=p).resolve()
        assert cred.token == "dbtc_emea"


class TestPkce:
    def test_verifier_length(self):
        verifier, _ = generate_pkce()
        assert len(verifier) >= 43

    def test_challenge_matches_sha256(self):
        import hashlib

        verifier, challenge = generate_pkce()
        expected = (
            base64.urlsafe_b64encode(hashlib.sha256(verifier.encode("ascii")).digest())
            .rstrip(b"=")
            .decode()
        )
        assert challenge == expected

    def test_verifier_is_url_safe(self):
        verifier, _ = generate_pkce()
        assert all(c.isalnum() or c in ("-", "_") for c in verifier)


class TestDecodeAccessToken:
    def test_happy_path(self):
        jwt = _make_fake_jwt(user_id=5001, account_id=1001, account_host="ab123.us1.dbt.com")
        user_id, account_id, account_host = decode_access_token(jwt)
        assert user_id == 5001
        assert account_id == 1001
        assert account_host == "ab123.us1.dbt.com"

    def test_invalid_jwt_not_three_parts(self):
        with pytest.raises(InteractiveAuthError, match="not a valid JWT"):
            decode_access_token("just.two")

    def test_missing_sub_claim(self):
        payload = json.dumps({"https://dbt.com/account_id": "1", "iss": "https://host"})
        body = base64.urlsafe_b64encode(payload.encode()).rstrip(b"=").decode()
        header = base64.urlsafe_b64encode(b'{"alg":"RS256"}').rstrip(b"=").decode()
        jwt = f"{header}.{body}.sig"

        with pytest.raises(InteractiveAuthError, match="sub"):
            decode_access_token(jwt)

    def test_missing_account_id_claim(self):
        payload = json.dumps({"sub": "1", "iss": "https://host"})
        body = base64.urlsafe_b64encode(payload.encode()).rstrip(b"=").decode()
        header = base64.urlsafe_b64encode(b'{"alg":"RS256"}').rstrip(b"=").decode()
        jwt = f"{header}.{body}.sig"

        with pytest.raises(InteractiveAuthError, match="account_id"):
            decode_access_token(jwt)

    def test_missing_iss_claim(self):
        payload = json.dumps({"sub": "1", "https://dbt.com/account_id": "1"})
        body = base64.urlsafe_b64encode(payload.encode()).rstrip(b"=").decode()
        header = base64.urlsafe_b64encode(b'{"alg":"RS256"}').rstrip(b"=").decode()
        jwt = f"{header}.{body}.sig"

        with pytest.raises(InteractiveAuthError, match="iss"):
            decode_access_token(jwt)
