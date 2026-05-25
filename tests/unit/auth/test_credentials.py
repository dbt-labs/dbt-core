import time

from dbt.auth.credentials import Credential, CredentialKind, OAuthSession


class TestCredentialFromToken:
    def test_service_token_classification(self):
        cred = Credential.from_token("dbtc_abc123", "ab123.us1.dbt.com", 42)
        assert cred.kind == CredentialKind.SERVICE_TOKEN
        assert cred.token == "dbtc_abc123"
        assert cred.account_host == "ab123.us1.dbt.com"
        assert cred.account_id == 42

    def test_pat_classification(self):
        cred = Credential.from_token("dbtu_user_token", "ab123.us1.dbt.com", 7)
        assert cred.kind == CredentialKind.PAT
        assert cred.token == "dbtu_user_token"

    def test_empty_token_is_service_token(self):
        cred = Credential.from_token("", "host", 1)
        assert cred.kind == CredentialKind.SERVICE_TOKEN

    def test_dbtu_prefix_only(self):
        cred = Credential.from_token("dbtu_", "host", 1)
        assert cred.kind == CredentialKind.PAT

    def test_similar_prefix_is_service_token(self):
        cred = Credential.from_token("dbtuser_abc", "host", 1)
        assert cred.kind == CredentialKind.SERVICE_TOKEN


class TestCredentialFromOAuth:
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
        cred = Credential.from_oauth(session)
        assert cred.kind == CredentialKind.OAUTH
        assert cred.token == "tok_abc"
        assert cred.account_host == "ab123.us1.dbt.com"
        assert cred.account_id == 42
        assert cred.oauth_session is session

    def test_from_oauth_with_optional_fields(self):
        session = OAuthSession(
            access_token="tok_abc",
            scopes=["account:read", "offline_access"],
            expires_at=time.time() + 3600,
            account_host="host",
            account_id=1,
            user_id=2,
            client_id="cli",
            refresh_token="refresh_tok",
            id_token="id_tok",
        )
        cred = Credential.from_oauth(session)
        assert cred.oauth_session.refresh_token == "refresh_tok"
        assert cred.oauth_session.id_token == "id_tok"


class TestOAuthSession:
    def test_defaults(self):
        session = OAuthSession(
            access_token="tok",
            scopes=[],
            expires_at=0.0,
            account_host="host",
            account_id=1,
            user_id=2,
            client_id="cli",
        )
        assert session.refresh_token is None
        assert session.id_token is None
