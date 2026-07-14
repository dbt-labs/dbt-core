from unittest import mock

from dbt.artifacts.schemas.results import RunStatus
from dbt.contracts.graph.nodes import SavedQuery
from dbt.hints import HintType
from dbt.node_types import NodeType
from dbt.runners import SavedQueryRunner
from dbt.task.build import BuildTask


def _model_node():
    return mock.Mock(resource_type=NodeType.Model)


def _test_node():
    return mock.Mock(resource_type=NodeType.Test)


class TestBuildReuseRelationsHint:
    def _run_before_run(self, flattened_nodes):
        task = BuildTask.__new__(BuildTask)
        task._flattened_nodes = flattened_nodes
        # No state/deferral in play, so the hint is eligible on count alone.
        task.args = mock.Mock(state=None, defer=False, defer_state=None)
        with mock.patch("dbt.task.build.show_hint") as mock_show_hint:
            # Skip the real (adapter-driven) before_run body.
            with mock.patch(
                "dbt.task.run.RunTask.before_run", return_value=RunStatus.Success
            ) as mock_super:
                result = task.before_run(adapter=mock.Mock(), selected_uids=set())
        assert result == RunStatus.Success
        mock_super.assert_called_once()
        return mock_show_hint

    def test_hint_fires_above_threshold(self):
        nodes = [_model_node() for _ in range(101)]
        mock_show_hint = self._run_before_run(nodes)
        mock_show_hint.assert_called_once_with(HintType.REUSE_RELATIONS_ON_TOO_MANY_MODELS)

    def test_hint_not_fired_at_threshold(self):
        nodes = [_model_node() for _ in range(100)]
        mock_show_hint = self._run_before_run(nodes)
        mock_show_hint.assert_not_called()

    def test_only_models_counted(self):
        # 100 models + many tests: still at (not above) the model threshold.
        nodes = [_model_node() for _ in range(100)] + [_test_node() for _ in range(50)]
        mock_show_hint = self._run_before_run(nodes)
        mock_show_hint.assert_not_called()

    def test_no_nodes(self):
        mock_show_hint = self._run_before_run(None)
        mock_show_hint.assert_not_called()


def test_saved_query_runner_on_skip(saved_query: SavedQuery):
    runner = SavedQueryRunner(
        config=None,
        adapter=None,
        node=saved_query,
        node_index=None,
        num_nodes=None,
    )
    # on_skip would work
    runner.on_skip()
