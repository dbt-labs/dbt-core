import datetime
import tempfile
from unittest import mock

import pytest

import dbt.tracking
from dbt.compilation import _generate_stats, print_compile_stats
from dbt.node_types import NodeType


@pytest.fixture(scope="function")
def active_user_none() -> None:
    dbt.tracking.active_user = None


@pytest.fixture(scope="function")
def tempdir(active_user_none) -> str:
    return tempfile.mkdtemp()


class TestTracking:
    def test_tracking_initial(self, tempdir):
        assert dbt.tracking.active_user is None
        dbt.tracking.initialize_from_flags(True, tempdir)
        assert isinstance(dbt.tracking.active_user, dbt.tracking.User)

        invocation_id = dbt.tracking.active_user.invocation_id
        run_started_at = dbt.tracking.active_user.run_started_at

        assert dbt.tracking.active_user.do_not_track is False
        assert isinstance(dbt.tracking.active_user.id, str)
        assert isinstance(invocation_id, str)
        assert isinstance(run_started_at, datetime.datetime)

        dbt.tracking.disable_tracking()
        assert isinstance(dbt.tracking.active_user, dbt.tracking.User)

        assert dbt.tracking.active_user.do_not_track is True
        assert dbt.tracking.active_user.id is None
        assert dbt.tracking.active_user.invocation_id == invocation_id
        assert dbt.tracking.active_user.run_started_at == run_started_at

        # this should generate a whole new user object -> new run_started_at
        dbt.tracking.do_not_track()
        assert isinstance(dbt.tracking.active_user, dbt.tracking.User)

        assert dbt.tracking.active_user.do_not_track is True
        assert dbt.tracking.active_user.id is None
        assert isinstance(dbt.tracking.active_user.invocation_id, str)
        assert isinstance(dbt.tracking.active_user.run_started_at, datetime.datetime)
        # invocation_id no longer only linked to active_user so it doesn't change
        assert dbt.tracking.active_user.invocation_id == invocation_id
        # if you use `!=`, you might hit a race condition (especially on windows)
        assert dbt.tracking.active_user.run_started_at is not run_started_at

    def test_tracking_never_ok(self, active_user_none):
        assert dbt.tracking.active_user is None

        # this should generate a whole new user object -> new invocation_id/run_started_at
        dbt.tracking.do_not_track()
        assert isinstance(dbt.tracking.active_user, dbt.tracking.User)

        assert dbt.tracking.active_user.do_not_track is True
        assert dbt.tracking.active_user.id is None
        assert isinstance(dbt.tracking.active_user.invocation_id, str)
        assert isinstance(dbt.tracking.active_user.run_started_at, datetime.datetime)

    def test_disable_never_enabled(self, active_user_none):
        assert dbt.tracking.active_user is None

        # this should generate a whole new user object -> new invocation_id/run_started_at
        dbt.tracking.disable_tracking()
        assert isinstance(dbt.tracking.active_user, dbt.tracking.User)

        assert dbt.tracking.active_user.do_not_track is True
        assert dbt.tracking.active_user.id is None
        assert isinstance(dbt.tracking.active_user.invocation_id, str)
        assert isinstance(dbt.tracking.active_user.run_started_at, datetime.datetime)

    @pytest.mark.parametrize("send_anonymous_usage_stats", [True, False])
    def test_initialize_from_flags(self, tempdir, send_anonymous_usage_stats):
        dbt.tracking.initialize_from_flags(send_anonymous_usage_stats, tempdir)
        assert dbt.tracking.active_user.do_not_track != send_anonymous_usage_stats


class TestCompileStatsTracking:
    def test_generate_stats_includes_catalog_count(self) -> None:
        mock_manifest = mock.MagicMock()
        stats = _generate_stats(mock_manifest, catalogs=["cat_a", "cat_b"])
        assert stats["catalogs"] == 2

        stats_no_catalogs = _generate_stats(mock_manifest, catalogs=None)
        assert "catalogs" not in stats_no_catalogs

    def test_print_compile_stats_tracks_catalog_count(self) -> None:
        mock_user = mock.Mock(do_not_track=False)
        with mock.patch("dbt.tracking.active_user", mock_user):
            with mock.patch("dbt.tracking.track_resource_counts") as mock_track:
                with mock.patch("dbt.compilation.fire_event"):
                    stats = {NodeType.Model: 1, "catalogs": 3}
                    print_compile_stats(stats)
        mock_track.assert_called_once()
        resource_counts = mock_track.call_args[0][0]
        assert resource_counts["catalogs"] == 3
        assert resource_counts["models"] == 1
