import pytest

from tests.adapter.dbt.tests.adapter.materialized_views.test_materialized_views import (
    MaterializedViewTestsBase,
)


class TestMaterializedViews(MaterializedViewTestsBase):
    @pytest.mark.skip(
        "This needs to be skipped for now because we're stubbing out with traditional views"
    )
    def test_updated_base_table_data_only_shows_in_materialized_view_after_rerun(self, project):
        pass
