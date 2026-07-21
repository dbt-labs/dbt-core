from dbt.contracts.graph.nodes import SavedQuery
from dbt.runners import SavedQueryRunner


class TestBuildUnitTestNodeCount:
    """Unit tests run alongside their model rather than as their own queue
    nodes, so their count is added to num_nodes in handle_job_queue. That
    addition must happen exactly once -- see dbt-core#11185, where gating on
    run_count (only bumped by the async workers, so still 0 across several
    dequeues) inflated the "X of Y" node count.
    """

    def _make_task(self, num_nodes, selected_unit_tests):
        task = BuildTask.__new__(BuildTask)
        task.num_nodes = num_nodes
        task.selected_unit_tests = selected_unit_tests
        task._unit_test_count_added = False
        task.run_count = 0
        task.model_to_unit_test_map = {}
        task.job_queue = mock.Mock()
        task.job_queue.get.return_value = _test_node()
        # Isolate the num_nodes bookkeeping from the actual dispatch.
        task.handle_job_queue_node = mock.Mock()
        return task

    def test_count_added_once_across_dequeues(self):
        task = self._make_task(num_nodes=3, selected_unit_tests={"ut.1", "ut.2"})
        # Several dequeues happen before any worker bumps run_count.
        for _ in range(3):
            task.handle_job_queue(pool=mock.Mock(), callback=mock.Mock())
        assert task.num_nodes == 5

    def test_no_unit_tests_is_noop(self):
        task = self._make_task(num_nodes=4, selected_unit_tests=set())
        task.handle_job_queue(pool=mock.Mock(), callback=mock.Mock())
        assert task.num_nodes == 4


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
