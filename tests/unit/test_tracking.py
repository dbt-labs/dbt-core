import datetime
import tempfile
from unittest import mock
from unittest.mock import MagicMock, patch

import pytest

import dbt.tracking
from dbt.adapters.base import AdapterTrackingRelationInfo
from dbt.artifacts.schemas.results import RunStatus
from dbt.artifacts.schemas.run import RunResult
from dbt.compilation import _generate_stats, print_compile_stats
from dbt.exceptions import DbtInternalError
from dbt.node_types import NodeType
from dbt.task.run import track_model_run


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


<<<<<<< HEAD
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


class TestTrackModelRun:
    def test_raises_without_active_user(self, active_user_none) -> None:
        node = mock.MagicMock(resource_type=NodeType.Model)
        result = RunResult(
            status=RunStatus.Success,
            timing=[],
            thread_id="t",
            execution_time=0.0,
            adapter_response={},
            message=None,
            failures=None,
            batch_results=None,
            node=node,
        )
        with pytest.raises(DbtInternalError, match="cannot track model run"):
            track_model_run(0, 1, result)

    @mock.patch("dbt.tracking.track_model_run")
    @mock.patch("dbt.task.run.get_invocation_id", return_value="inv-1")
    @mock.patch("dbt.task.run.utils.get_hash", return_value="mh")
    @mock.patch("dbt.task.run.utils.get_hashed_contents", return_value="mhc")
    @mock.patch.object(dbt.tracking, "active_user", new_callable=mock.Mock)
    def test_forwards_payload_to_tracking(
        self,
        _active_user,
        _get_hashed_contents,
        _get_hash,
        _get_invocation_id,
        mock_track,
    ) -> None:
        node = mock.MagicMock()
        node.resource_type = NodeType.Model
        node.access = None
        node.contract.enforced = False
        node.version = None
        node.config.incremental_strategy = "merge"
        node.config._extra = {}
        node.get_materialization.return_value = "table"
        node.language = "sql"

        result = RunResult(
            status=RunStatus.Skipped,
            timing=[],
            thread_id="t",
            execution_time=2.0,
            adapter_response={},
            message=None,
            failures=None,
            batch_results=None,
            node=node,
        )

        track_model_run(3, 10, result, adapter=None)

        mock_track.assert_called_once()
        opts = mock_track.call_args[0][0]
        assert opts["invocation_id"] == "inv-1"
        assert opts["index"] == 3
        assert opts["total"] == 10
        assert opts["execution_time"] == 2.0
        assert opts["run_skipped"] is True
        assert opts["run_error"] is False
        assert opts["model_incremental_strategy"] == "merge"
        assert opts["catalog_type"] is None
        assert opts["adapter_info"] == {}

    @mock.patch("dbt.tracking.track_model_run")
    @mock.patch("dbt.task.run.get_invocation_id", return_value="inv-1")
    @mock.patch("dbt.task.run.utils.get_hash", return_value="mh")
    @mock.patch("dbt.task.run.utils.get_hashed_contents", return_value="mhc")
    @mock.patch.object(dbt.tracking, "active_user", new_callable=mock.Mock)
    def test_forwards_catalog_type_from_adapter_integration(
        self,
        _active_user,
        _get_hashed_contents,
        _get_hash,
        _get_invocation_id,
        mock_track,
    ) -> None:
        node = mock.MagicMock()
        node.resource_type = NodeType.Model
        node.access = None
        node.contract.enforced = False
        node.version = None
        node.config.incremental_strategy = None
        node.config._extra = {"catalog_name": "test_catalog"}
        node.get_materialization.return_value = "table"
        node.language = "sql"

        adapter = mock.MagicMock()
        adapter.get_adapter_run_info.return_value = AdapterTrackingRelationInfo(
            adapter_name="snowflake",
            base_adapter_version="0",
            adapter_version="0",
            model_adapter_details={},
        )
        integration = mock.Mock(catalog_type="ICEBERG_REST")
        adapter.get_catalog_integration.return_value = integration

        result = RunResult(
            status=RunStatus.Success,
            timing=[],
            thread_id="t",
            execution_time=1.0,
            adapter_response={},
            message=None,
            failures=None,
            batch_results=None,
            node=node,
        )

        track_model_run(0, 1, result, adapter=adapter)

        mock_track.assert_called_once()
        opts = mock_track.call_args[0][0]
        assert opts["catalog_type"] == "ICEBERG_REST"
        adapter.get_catalog_integration.assert_called_once_with("test_catalog")


class TestTimeoutEmitter:
    """Verify that the TimeoutEmitter uses short timeouts so an unreachable
    collector doesn't stall dbt at the end of every invocation.
    See https://github.com/dbt-labs/dbt-core/issues/9989
    """

    def test_http_post_uses_short_timeout(self):
        emitter = dbt.tracking.TimeoutEmitter()
        mock_response = MagicMock()
        mock_response.status_code = 200

        with patch("dbt.tracking.requests.post", return_value=mock_response) as mock_post:
            emitter.http_post('{"test": "payload"}')
            _, kwargs = mock_post.call_args
            assert "timeout" in kwargs
            assert kwargs["timeout"] <= 2.0, (
                f"POST timeout {kwargs['timeout']}s is too long; keep it ≤ 2s so an unreachable "
                "collector doesn't stall dbt at the end of every invocation."
            )

    def test_http_get_uses_short_timeout(self):
        emitter = dbt.tracking.TimeoutEmitter()
        mock_response = MagicMock()
        mock_response.status_code = 200

        with patch("dbt.tracking.requests.get", return_value=mock_response) as mock_get:
            emitter.http_get({"test": "payload"})
            _, kwargs = mock_get.call_args
            assert "timeout" in kwargs
            assert kwargs["timeout"] <= 2.0, (
                f"GET timeout {kwargs['timeout']}s is too long; keep it ≤ 2s so an unreachable "
                "collector doesn't stall dbt at the end of every invocation."
            )
