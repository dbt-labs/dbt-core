import datetime
import tempfile

import pytest

import dbt.tracking


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
