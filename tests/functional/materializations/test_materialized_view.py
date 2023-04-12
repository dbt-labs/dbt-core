from tests.adapter.dbt.tests.adapter.materialized_views.test_materialized_views import (
    MaterializedViewTestsBase,
)


class TestMaterializedViews(MaterializedViewTestsBase):
    def test_index_gets_updated(self, project):
        """
        we'll need a more complicated model for this
        (there's only one column in the default, which means only one index)

        - make sure an index was created on initial dbt_run
        - update the index in the config
        - rerun the materialized view
        - confirm that the new index exists and the old one is gone
        """
        pass
