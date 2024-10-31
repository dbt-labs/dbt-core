from dbt.contracts.graph.nodes import SavedQuery
from dbt.task.build import SavedQueryRunner


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
