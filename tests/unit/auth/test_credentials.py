import time

from dbt.auth.credentials import OAuthSession, PlatformCredential, RuncacheCredential


class TestPlatformCredentialFromToken:
    def test_static_token_never_expires(self):
        cred = PlatformCredential.from_token("tok", "host", 1)
        assert cred.valid
        assert not cred.expired


class TestPlatformCredentialFromOAuth:
    def test_from_oauth(self):
        session = OAuthSession(
            access_token="tok_abc",
            scopes=["account:read"],
            expires_at=time.time() + 3600,
            account_host="ab123.us1.dbt.com",
            account_id=42,
            user_id=7,
            client_id="test_client",
        )
        cred = PlatformCredential.from_oauth(session)
        assert cred.token == "tok_abc"
        assert cred.account_host == "ab123.us1.dbt.com"
        assert cred.account_id == 42
        assert cred.oauth_session is session

    def test_expired_oauth_credential(self):
        session = OAuthSession(
            access_token="tok",
            scopes=[],
            expires_at=time.time() - 100,
            account_host="host",
            account_id=1,
            user_id=2,
            client_id="cli",
        )
        cred = PlatformCredential.from_oauth(session)
        assert cred.expired
        assert not cred.valid


class TestRuncacheCredential:
    def test_from_token_response(self):
        token_data = {
            "access_token": "rc_tok",
            "refresh_token": "rc_ref",
            "id_token": "rc_id",
            "expires_in": 900,
            "scope": "runcache:scope:orgs",
        }
        cred = RuncacheCredential.from_token_response(token_data)
        assert cred.token == "rc_tok"
        assert cred.refresh_token == "rc_ref"
        assert cred.scopes == ["runcache:scope:orgs"]
        assert cred.valid

    def test_apply_headers(self):
        token_data = {"access_token": "rc_tok", "expires_in": 900}
        cred = RuncacheCredential.from_token_response(token_data)
        headers = {}
        cred.apply(headers)
        assert headers["Authorization"] == "Bearer rc_tok"


class TestCredentialBase:
    def test_apply(self):
        cred = PlatformCredential.from_token("tok", "host", 1)
        headers = {}
        cred.apply(headers)
        assert headers["Authorization"] == "Bearer tok"

    def test_valid_with_token(self):
        cred = PlatformCredential.from_token("tok", "host", 1)
        assert cred.valid

    def test_invalid_without_token(self):
        cred = PlatformCredential.from_token("", "host", 1)
        assert not cred.valid
