import json
import os
import stat
import time

import pytest

from dbt.auth.credentials import OAuthSession
from dbt.auth.session_cache import OAuthSessionCache, read_session_cache, upsert_session
from dbt.exceptions import InaccessibleSource, MalformedAuthConfig


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


class TestOAuthSessionCache:
    def test_sessions_are_independent_per_instance(self):
        cache_a = OAuthSessionCache()
        cache_b = OAuthSessionCache()
        cache_a.sessions.append(_make_session())
        assert len(cache_b.sessions) == 0

    def test_with_sessions(self):
        s1 = _make_session(account_id=1)
        s2 = _make_session(account_id=2)
        cache = OAuthSessionCache(version=1, sessions=[s1, s2])
        assert len(cache.sessions) == 2
        assert cache.sessions[0].account_id == 1
        assert cache.sessions[1].account_id == 2

    def test_to_dict_round_trip(self):
        session = _make_session()
        cache = OAuthSessionCache(version=1, sessions=[session])
        data = cache.to_dict()
        restored = OAuthSessionCache.from_dict(data)
        assert restored.version == 1
        assert len(restored.sessions) == 1
        assert restored.sessions[0].access_token == "tok_abc"
        assert restored.sessions[0].account_id == 42

    def test_from_dict_empty(self):
        cache = OAuthSessionCache.from_dict({})
        assert cache.version == 1
        assert cache.sessions == []


class TestReadSessionCache:
    def test_missing_file_returns_empty_cache(self, tmp_path):
        cache = read_session_cache(tmp_path / "nonexistent.json")
        assert cache.version == 1
        assert cache.sessions == []

    def test_valid_file(self, tmp_path):
        session = _make_session()
        data = OAuthSessionCache(sessions=[session]).to_dict()
        p = tmp_path / "oauth_sessions.json"
        p.write_text(json.dumps(data))

        cache = read_session_cache(p)
        assert len(cache.sessions) == 1
        assert cache.sessions[0].access_token == "tok_abc"

    def test_empty_sessions_file(self, tmp_path):
        p = tmp_path / "oauth_sessions.json"
        p.write_text(json.dumps({"version": 1, "sessions": []}))

        cache = read_session_cache(p)
        assert cache.sessions == []

    def test_malformed_json_raises(self, tmp_path):
        p = tmp_path / "oauth_sessions.json"
        p.write_text("not json {{{")

        with pytest.raises(MalformedAuthConfig, match="invalid JSON"):
            read_session_cache(p)

    def test_non_object_json_raises(self, tmp_path):
        p = tmp_path / "oauth_sessions.json"
        p.write_text('"just a string"')

        with pytest.raises(MalformedAuthConfig, match="expected object"):
            read_session_cache(p)

    @pytest.mark.skipif(os.name == "nt", reason="Unix permissions")
    def test_unreadable_file_raises_inaccessible(self, tmp_path):
        p = tmp_path / "oauth_sessions.json"
        p.write_text("{}")
        p.chmod(0o000)
        try:
            with pytest.raises(InaccessibleSource):
                read_session_cache(p)
        finally:
            p.chmod(stat.S_IRUSR | stat.S_IWUSR)


class TestUpsertSession:
    def test_creates_file_if_missing(self, tmp_path):
        p = tmp_path / "subdir" / "oauth_sessions.json"
        session = _make_session()

        upsert_session(session, p)

        assert p.exists()
        cache = read_session_cache(p)
        assert len(cache.sessions) == 1
        assert cache.sessions[0].access_token == "tok_abc"

    def test_replaces_existing_session(self, tmp_path):
        p = tmp_path / "oauth_sessions.json"
        session1 = _make_session(access_token="old_tok")
        upsert_session(session1, p)

        session2 = _make_session(access_token="new_tok")
        upsert_session(session2, p)

        cache = read_session_cache(p)
        assert len(cache.sessions) == 1
        assert cache.sessions[0].access_token == "new_tok"

    def test_appends_different_account(self, tmp_path):
        p = tmp_path / "oauth_sessions.json"
        session1 = _make_session(account_id=1)
        session2 = _make_session(account_id=2)

        upsert_session(session1, p)
        upsert_session(session2, p)

        cache = read_session_cache(p)
        assert len(cache.sessions) == 2

    def test_appends_different_client(self, tmp_path):
        p = tmp_path / "oauth_sessions.json"
        session1 = _make_session(client_id="client_a")
        session2 = _make_session(client_id="client_b")

        upsert_session(session1, p)
        upsert_session(session2, p)

        cache = read_session_cache(p)
        assert len(cache.sessions) == 2

    def test_valid_json_output(self, tmp_path):
        p = tmp_path / "oauth_sessions.json"
        upsert_session(_make_session(), p)

        data = json.loads(p.read_text())
        assert "version" in data
        assert "sessions" in data
        assert isinstance(data["sessions"], list)
