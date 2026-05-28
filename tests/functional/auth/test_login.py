"""Functional tests for `dbt login`.

Tests exercise the full CLI path via run_dbt, mocking only the
network boundary (token exchange, JWKS, features API) and the callback
server (which would block waiting for a real browser redirect).
"""

from __future__ import annotations

import json
from unittest import mock

import pytest

from dbt.auth.session_cache import read_session_cache
from dbt.config.user_settings import get_user_setting_flags
from dbt.tests.util import run_dbt
from dbt_common.events.event_catcher import EventCatcher
from dbt_common.events.types import Note

STATE_TOKEN_PARAM = {
    "flow": "state",
    "token_data": {
        "access_token": "state_access_tok",
        "refresh_token": "state_refresh_tok",
        "id_token": "state_id_tok",
        "scope": "runcache:scope:orgs",
        "expires_in": 900,
    },
}


@pytest.mark.usefixtures(
    "no_browser",
    "stub_callback_server",
    "stub_confirm",
    "redirect_cache_paths",
    "stub_platform_api",
    "mock_flags",
)
class TestLoginInteractivePlatform:
    """dbt login -> browser OAuth -> platform credential persisted."""

    @pytest.fixture(autouse=True)
    def _stub_platform_exchange(self, fake_jwt):
        token_data = {
            "access_token": fake_jwt,
            "refresh_token": "refresh_tok",
            "id_token": "id_tok",
            "scope": "account:read identity:read offline_access",
            "expires_in": 3600,
        }
        resp = mock.Mock()
        resp.ok = True
        resp.status_code = 200
        resp.raise_for_status = mock.Mock()
        resp.json.return_value = token_data
        resp.text = json.dumps(token_data)
        with mock.patch("dbt.auth.oauth.platform.requests.post", return_value=resp):
            yield

    def test_login_exchanges_code_and_persists_session(
        self,
        redirect_cache_paths,
        fake_jwt,
    ):
        catcher = EventCatcher(Note)
        run_dbt(["login"], callbacks=[catcher.catch])

        msgs = [e.info.msg for e in catcher.caught_events]
        assert any("Opening your browser to complete login" in m for m in msgs)
        assert any("reset your password" in m for m in msgs)
        assert any("Congratulations! You are now signed in." in m for m in msgs)

        cache = read_session_cache(redirect_cache_paths)
        assert len(cache.sessions) == 1
        assert cache.sessions[0].access_token == fake_jwt
        assert cache.sessions[0].account_id == 42

    @pytest.mark.parametrize("stub_platform_api", [True], indirect=True)
    def test_state_configured_remotely_user_confirms(
        self,
        stub_platform_api,
        stub_confirm,
    ):
        catcher = EventCatcher(Note)
        run_dbt(["login"], callbacks=[catcher.catch])

        stub_confirm.assert_called_once()
        assert "Enable state on this machine" in stub_confirm.call_args[0][0]
        assert get_user_setting_flags()["manage_state"] is True

    @pytest.mark.parametrize("stub_platform_api", [True], indirect=True)
    @pytest.mark.parametrize("stub_confirm", [False], indirect=True)
    def test_state_configured_remotely_user_declines(
        self,
        stub_platform_api,
        stub_confirm,
    ):
        catcher = EventCatcher(Note)
        run_dbt(["login"], callbacks=[catcher.catch])

        assert any(
            "you can modify ~/.dbt/user_settings.yml" in e.info.msg for e in catcher.caught_events
        )
        assert not get_user_setting_flags().get("manage_state")

    @pytest.mark.parametrize("mock_flags", [True], indirect=True)
    def test_state_enabled_locally_but_not_remotely_warns(
        self,
        mock_flags,
    ):
        from dbt.config.user_settings import set_user_setting_flag

        set_user_setting_flag("manage_state", True)

        catcher = EventCatcher(Note)
        run_dbt(["login"], callbacks=[catcher.catch])

        assert any("not in your dbt platform account" in e.info.msg for e in catcher.caught_events)


@pytest.mark.usefixtures("redirect_cache_paths", "no_browser")
@pytest.mark.parametrize("stub_callback_server", ["error"], indirect=True)
class TestLoginAuthFailure:
    """dbt login -> authentication fails."""

    def test_auth_failure_shows_error(self, stub_callback_server):
        catcher = EventCatcher(Note)
        run_dbt(["login"], expect_pass=False, callbacks=[catcher.catch])

        assert any(
            "Authentication failed. Re-run dbt login to try again." in e.info.msg
            for e in catcher.caught_events
        )


@pytest.mark.usefixtures("redirect_cache_paths", "no_browser")
@pytest.mark.parametrize("stub_callback_server", ["state"], indirect=True)
@pytest.mark.parametrize("stub_token_exchange", [STATE_TOKEN_PARAM], indirect=True)
class TestLoginInteractiveState:
    """dbt login -> browser OAuth -> state credential."""

    def test_login_state_sets_manage_state_flag(
        self,
        stub_callback_server,
        stub_token_exchange,
        tmp_path,
    ):
        catcher = EventCatcher(Note)
        run_dbt(["login"], callbacks=[catcher.catch])

        assert any("dbt State login successful" in e.info.msg for e in catcher.caught_events)

        state_auth = json.loads((tmp_path / "state_auth.json").read_text())
        assert state_auth["access_token"] == "state_access_tok"
        assert get_user_setting_flags()["manage_state"] is True


@pytest.mark.usefixtures("seeded_cloud_yaml", "stub_platform_api", "mock_flags", "stub_confirm")
class TestLoginCloudYaml:
    """dbt login with existing dbt_cloud.yml credentials."""

    def test_login_resolves_from_cloud_yaml(self, no_browser):
        catcher = EventCatcher(Note)

        run_dbt(["login"], callbacks=[catcher.catch])

        no_browser.assert_not_called()
        assert any(
            "Congratulations! You are now signed in." in e.info.msg for e in catcher.caught_events
        )


@pytest.mark.usefixtures("seeded_oauth_cache", "stub_platform_api", "mock_flags", "stub_confirm")
class TestLoginPassiveShortCircuit:
    """dbt login with a valid cached session resolves without browser."""

    def test_cached_session_skips_browser(self, no_browser):
        catcher = EventCatcher(Note)

        run_dbt(["login"], callbacks=[catcher.catch])

        no_browser.assert_not_called()
        assert any(
            "Congratulations! You are now signed in." in e.info.msg for e in catcher.caught_events
        )
