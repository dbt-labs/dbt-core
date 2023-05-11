from dbt.tests.util import run_dbt

from dbt.tests.adapter.materialized_views.base import Base


class BasicTestsBase(Base):
    def test_relation_is_materialized_view_on_initial_creation(self, project):
        self.assert_relation_is_materialized_view(project, self.base_materialized_view)

    def test_relation_is_materialized_view_when_rerun(self, project):
        run_dbt(["run", "--models", self.base_materialized_view.name])
        self.assert_relation_is_materialized_view(project, self.base_materialized_view)

    def test_relation_is_materialized_view_on_full_refresh(self, project):
        run_dbt(["run", "--models", self.base_materialized_view.name, "--full-refresh"])
        self.assert_relation_is_materialized_view(project, self.base_materialized_view)

    def test_relation_is_materialized_view_on_update(self, project):
        run_dbt(
            [
                "run",
                "--models",
                self.base_materialized_view.name,
                "--vars",
                "quoting: {identifier: True}",
            ]
        )
        self.assert_relation_is_materialized_view(project, self.base_materialized_view)

    def test_updated_base_table_data_only_shows_in_materialized_view_after_rerun(self, project):
        self.insert_records(project, self.inserted_records)
        assert self.get_records(project, self.base_materialized_view) == self.starting_records

        run_dbt(["run", "--models", self.base_materialized_view.name])
        final_records = self.starting_records + self.inserted_records
        assert self.get_records(project, self.base_materialized_view) == final_records
