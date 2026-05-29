"""Functional tests for `dbt login status`."""

from __future__ import annotations

from pathlib import Path
from unittest import mock

import pytest

from dbt.tests.util import run_dbt
from dbt_common.events.event_catcher import EventCatcher
from dbt_common.events.types import Note


class TestLoginStatus:
    """dbt login status subcommand."""

    @pytest.mark.usefixtures("seeded_oauth_cache")
    def test_status_shows_authenticated_via_oauth(self):
        catcher = EventCatcher(Note)
        run_dbt(["login", "status"], callbacks=[catcher.catch])

        status_msg = next(
            (e.info.msg for e in catcher.caught_events if "Status:" in e.info.msg), None
        )
        assert status_msg is not None
        assert "authenticated" in status_msg
        assert "ab123.us1.dbt.com" in status_msg
        assert "42" in status_msg

    @pytest.mark.usefixtures("seeded_cloud_yaml")
    def test_status_shows_authenticated_via_cloud_yaml(self):
        catcher = EventCatcher(Note)
        run_dbt(["login", "status"], callbacks=[catcher.catch])

        status_msg = next(
            (e.info.msg for e in catcher.caught_events if "Status:" in e.info.msg), None
        )
        assert status_msg is not None
        assert "authenticated" in status_msg
        assert "dbt_cloud.yml" in status_msg
        assert "ab123.us1.dbt.com" in status_msg
        assert "42" in status_msg

    @pytest.mark.usefixtures("redirect_cache_paths")
    def test_status_shows_unauthenticated(self, tmp_path):
        catcher = EventCatcher(Note)

        with mock.patch.dict(
            "os.environ",
            {"DBT_CLOUD_ACCOUNT_HOST": "", "DBT_CLOUD_TOKEN": "", "DBT_CLOUD_ACCOUNT_ID": ""},
        ), mock.patch(
            "dbt.auth.resolvers.CloudYamlResolver._default_path",
            return_value=tmp_path / "nonexistent.yml",
        ):
            run_dbt(["login", "status"], expect_pass=False, callbacks=[catcher.catch])

        status_msg = next(
            (e.info.msg for e in catcher.caught_events if "Status:" in e.info.msg), None
        )
        assert status_msg is not None
        assert "unauthenticated" in status_msg

    @pytest.mark.usefixtures("redirect_cache_paths")
    def test_status_shows_authenticated_via_env_vars(self):
        catcher = EventCatcher(Note)

        with mock.patch.dict(
            "os.environ",
            {
                "DBT_CLOUD_ACCOUNT_HOST": "cloud.getdbt.com",
                "DBT_CLOUD_TOKEN": "tok_abc123",
                "DBT_CLOUD_ACCOUNT_ID": "99",
            },
        ):
            run_dbt(["login", "status"], callbacks=[catcher.catch])

        status_msg = next(
            (e.info.msg for e in catcher.caught_events if "Status:" in e.info.msg), None
        )
        assert status_msg is not None
        assert "environment variables" in status_msg
        assert "cloud.getdbt.com" in status_msg

    @pytest.mark.usefixtures("seeded_state_auth")
    def test_status_shows_authenticated_via_state_auth(self):
        catcher = EventCatcher(Note)

        with mock.patch.dict(
            "os.environ",
            {"DBT_CLOUD_ACCOUNT_HOST": "", "DBT_CLOUD_TOKEN": "", "DBT_CLOUD_ACCOUNT_ID": ""},
        ), mock.patch(
            "dbt.auth.resolvers.CloudYamlResolver._default_path",
            return_value=Path("/nonexistent/dbt_cloud.yml"),
        ):
            run_dbt(["login", "status"], callbacks=[catcher.catch])

        status_msg = next(
            (e.info.msg for e in catcher.caught_events if "dbt State" in e.info.msg), None
        )
        assert status_msg is not None
        assert "authenticated" in status_msg
