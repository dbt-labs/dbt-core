from dbt.tests.util import run_dbt

from dbt.tests.adapter.materialized_views.base import Base


class BasicTestsBase(Base):
    def test_relation_is_materialized_view_on_initial_creation(self, project):
        self.assert_relation_is_materialized_view(project, self.materialized_view)

    def test_relation_is_materialized_view_when_rerun(self, project):
        run_dbt(["run", "--models", self.materialized_view])
        self.assert_relation_is_materialized_view(project, self.materialized_view)

    def test_relation_is_materialized_view_on_full_refresh(self, project):
        run_dbt(["run", "--models", self.materialized_view, "--full-refresh"])
        self.assert_relation_is_materialized_view(project, self.materialized_view)

    def test_relation_is_materialized_view_on_update(self, project):
        run_dbt(
            ["run", "--models", self.materialized_view, "--vars", "quoting: {identifier: True}"]
        )
        self.assert_relation_is_materialized_view(project, self.materialized_view)

    def test_updated_base_table_data_only_shows_in_materialized_view_after_rerun(self, project):
        self.insert_records(project, self.inserted_records)
        assert self.get_records(project, self.materialized_view) == self.starting_records

        run_dbt(["run", "--models", self.materialized_view])
        final_records = self.starting_records + self.inserted_records
        assert self.get_records(project, self.materialized_view) == final_records
