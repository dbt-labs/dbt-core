from typing import Optional, Tuple

import pytest

from dbt.adapters.base.relation import BaseRelation
from dbt.contracts.relation import RelationType
from dbt.tests.util import (
    assert_message_in_logs,
    get_model_file,
    run_dbt,
    run_dbt_and_capture,
    set_model_file,
)

from dbt.tests.adapter.materialized_view.files import (
    MY_MATERIALIZED_VIEW,
    MY_SEED,
    MY_TABLE,
    MY_VIEW,
)


class MaterializedViewBasic:
    """
    Tests basic functionality:

    - create
    - idempotent create
    - full refresh
    - materialized views can replace table/view
    - table/view can replace materialized views
    - the object is an actual materialized view, not a traditional view
    """

    """
    Configure these
    """

    @staticmethod
    def insert_record(project, table: BaseRelation, record: Tuple[int, int]):
        raise NotImplementedError(
            "To use this test, please implement `insert_record`, inherited from `MaterializedViewsBasic`."
        )

    @staticmethod
    def refresh_materialized_view(project, materialized_view: BaseRelation):
        raise NotImplementedError(
            "To use this test, please implement `refresh_materialized_view`, inherited from `MaterializedViewsBasic`."
        )

    @staticmethod
    def query_row_count(project, relation: BaseRelation) -> int:
        raise NotImplementedError(
            "To use this test, please implement `query_row_count`, inherited from `MaterializedViewsBasic`."
        )

    @staticmethod
    def query_relation_type(project, relation: BaseRelation) -> Optional[str]:
        raise NotImplementedError(
            "To use this test, please implement `query_relation_type`, inherited from `MaterializedViewsBasic`."
        )

    """
    Configure these if needed
    """

    @pytest.fixture(scope="class", autouse=True)
    def seeds(self):
        return {"my_seed.csv": MY_SEED}

    @pytest.fixture(scope="class", autouse=True)
    def models(self):
        yield {
            "my_table.sql": MY_TABLE,
            "my_view.sql": MY_VIEW,
            "my_materialized_view.sql": MY_MATERIALIZED_VIEW,
        }

    """
    Don't configure these unless absolutely necessary
    """

    @pytest.fixture(scope="class")
    def my_materialized_view(self, project) -> BaseRelation:
        return project.adapter.Relation.create(
            identifier="my_materialized_view",
            schema=project.test_schema,
            database=project.database,
            type=RelationType.MaterializedView,
        )

    @pytest.fixture(scope="class")
    def my_view(self, project) -> BaseRelation:
        return project.adapter.Relation.create(
            identifier="my_view",
            schema=project.test_schema,
            database=project.database,
            type=RelationType.View,
        )

    @pytest.fixture(scope="class")
    def my_table(self, project) -> BaseRelation:
        return project.adapter.Relation.create(
            identifier="my_table",
            schema=project.test_schema,
            database=project.database,
            type=RelationType.Table,
        )

    @pytest.fixture(scope="class")
    def my_seed(self, project) -> BaseRelation:
        return project.adapter.Relation.create(
            identifier="my_seed",
            schema=project.test_schema,
            database=project.database,
            type=RelationType.Table,
        )

    @staticmethod
    def swap_table_to_materialized_view(project, table):
        initial_model = get_model_file(project, table)
        new_model = initial_model.replace(
            "materialized='table'", "materialized='materialized_view'"
        )
        set_model_file(project, table, new_model)

    @staticmethod
    def swap_view_to_materialized_view(project, view):
        initial_model = get_model_file(project, view)
        new_model = initial_model.replace(
            "materialized='view'", "materialized='materialized_view'"
        )
        set_model_file(project, view, new_model)

    @staticmethod
    def swap_materialized_view_to_table(project, materialized_view):
        initial_model = get_model_file(project, materialized_view)
        new_model = initial_model.replace(
            "materialized='materialized_view'", "materialized='table'"
        )
        set_model_file(project, materialized_view, new_model)

    @staticmethod
    def swap_materialized_view_to_view(project, materialized_view):
        initial_model = get_model_file(project, materialized_view)
        new_model = initial_model.replace(
            "materialized='materialized_view'", "materialized='view'"
        )
        set_model_file(project, materialized_view, new_model)

    @pytest.fixture(scope="function", autouse=True)
    def setup(self, project, my_materialized_view):

        run_dbt(["seed"])
        run_dbt(["run", "--models", my_materialized_view.identifier, "--full-refresh"])

        # the tests touch these files, store their contents in memory
        initial_model = get_model_file(project, my_materialized_view)

        yield

        # and then reset them after the test runs
        set_model_file(project, my_materialized_view, initial_model)
        project.run_sql(f"drop schema if exists {project.test_schema} cascade")

    def test_materialized_view_create(self, project, my_materialized_view):
        # setup creates it; verify it's there
        assert self.query_relation_type(project, my_materialized_view) == "materialized_view"

    def test_materialized_view_create_idempotent(self, project, my_materialized_view):
        # setup creates it once; verify it's there and run once
        assert self.query_relation_type(project, my_materialized_view) == "materialized_view"
        run_dbt(["run", "--models", my_materialized_view.identifier])
        assert self.query_relation_type(project, my_materialized_view) == "materialized_view"

    def test_materialized_view_full_refresh(self, project, my_materialized_view):
        _, logs = run_dbt_and_capture(
            ["--debug", "run", "--models", my_materialized_view.identifier, "--full-refresh"]
        )
        assert self.query_relation_type(project, my_materialized_view) == "materialized_view"
        assert_message_in_logs(f"Applying REPLACE to: {my_materialized_view}", logs)

    def test_materialized_view_replaces_table(self, project, my_table):
        run_dbt(["run", "--models", my_table.identifier])
        assert self.query_relation_type(project, my_table) == "table"

        self.swap_table_to_materialized_view(project, my_table)

        run_dbt(["run", "--models", my_table.identifier])
        assert self.query_relation_type(project, my_table) == "materialized_view"

    def test_materialized_view_replaces_view(self, project, my_view):
        run_dbt(["run", "--models", my_view.identifier])
        assert self.query_relation_type(project, my_view) == "view"

        self.swap_view_to_materialized_view(project, my_view)

        run_dbt(["run", "--models", my_view.identifier])
        assert self.query_relation_type(project, my_view) == "materialized_view"

    def test_table_replaces_materialized_view(self, project, my_materialized_view):
        run_dbt(["run", "--models", my_materialized_view.identifier])
        assert self.query_relation_type(project, my_materialized_view) == "materialized_view"

        self.swap_materialized_view_to_table(project, my_materialized_view)

        run_dbt(["run", "--models", my_materialized_view.identifier])
        assert self.query_relation_type(project, my_materialized_view) == "table"

    def test_view_replaces_materialized_view(self, project, my_materialized_view):
        run_dbt(["run", "--models", my_materialized_view.identifier])
        assert self.query_relation_type(project, my_materialized_view) == "materialized_view"

        self.swap_materialized_view_to_view(project, my_materialized_view)

        run_dbt(["run", "--models", my_materialized_view.identifier])
        assert self.query_relation_type(project, my_materialized_view) == "view"

    def test_materialized_view_only_updates_after_refresh(
        self, project, my_materialized_view, my_seed
    ):
        # poll database
        table_start = self.query_row_count(project, my_seed)
        view_start = self.query_row_count(project, my_materialized_view)

        # insert new record in table
        self.insert_record(project, my_seed, (4, 400))

        # poll database
        table_mid = self.query_row_count(project, my_seed)
        view_mid = self.query_row_count(project, my_materialized_view)

        # refresh the materialized view
        self.refresh_materialized_view(project, my_materialized_view)

        # poll database
        table_end = self.query_row_count(project, my_seed)
        view_end = self.query_row_count(project, my_materialized_view)

        # new records were inserted in the table but didn't show up in the view until it was refreshed
        assert table_start < table_mid == table_end
        assert view_start == view_mid < view_end
