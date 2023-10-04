from typing import Optional, Tuple

import pytest

from dbt.adapters.base.relation import BaseRelation

from dbt.tests.util import get_model_file, set_model_file
from tests.adapter.dbt.tests.adapter.materialized_view.basic import MaterializedViewBasic
from tests.adapter.dbt.tests.adapter.materialized_view.changes import (
    MaterializedViewChanges,
    MaterializedViewChangesApplyMixin,
    MaterializedViewChangesContinueMixin,
    MaterializedViewChangesFailMixin,
)
from tests.adapter.dbt.tests.adapter.materialized_view.files import MY_TABLE, MY_VIEW
from tests.functional.materializations.materialized_view_tests.utils import (
    query_indexes,
    query_relation_type,
)


MY_MATERIALIZED_VIEW = """
{{ config(
    materialized='materialized_view',
    indexes=[{'columns': ['id']}],
) }}
select * from {{ ref('my_seed') }}
"""


class TestPostgresMaterializedViewsBasic(MaterializedViewBasic):
    @pytest.fixture(scope="class", autouse=True)
    def models(self):
        yield {
            "my_table.sql": MY_TABLE,
            "my_view.sql": MY_VIEW,
            "my_materialized_view.sql": MY_MATERIALIZED_VIEW,
        }

    @staticmethod
    def insert_record(project, table: BaseRelation, record: Tuple[int, int]):
        my_id, value = record
        project.run_sql(f"insert into {table} (id, value) values ({my_id}, {value})")

    @staticmethod
    def refresh_materialized_view(project, materialized_view: BaseRelation):
        sql = f"refresh materialized view {materialized_view}"
        project.run_sql(sql)

    @staticmethod
    def query_row_count(project, relation: BaseRelation) -> int:
        sql = f"select count(*) from {relation}"
        return project.run_sql(sql, fetch="one")[0]

    @staticmethod
    def query_relation_type(project, relation: BaseRelation) -> Optional[str]:
        return query_relation_type(project, relation)


class PostgresMaterializedViewChanges(MaterializedViewChanges):
    @pytest.fixture(scope="class", autouse=True)
    def models(self):
        yield {
            "my_table.sql": MY_TABLE,
            "my_view.sql": MY_VIEW,
            "my_materialized_view.sql": MY_MATERIALIZED_VIEW,
        }

    @staticmethod
    def query_relation_type(project, relation: BaseRelation) -> Optional[str]:
        return query_relation_type(project, relation)

    @staticmethod
    def check_start_state(project, materialized_view):
        indexes = query_indexes(project, materialized_view)
        assert len(indexes) == 1
        assert indexes[0]["column_names"] == "id"

    @staticmethod
    def change_config_via_alter(project, materialized_view):
        initial_model = get_model_file(project, materialized_view)
        new_model = initial_model.replace(
            "indexes=[{'columns': ['id']}]",
            "indexes=[{'columns': ['value']}]",
        )
        set_model_file(project, materialized_view, new_model)

    @staticmethod
    def check_state_alter_change_is_applied(project, materialized_view):
        indexes = query_indexes(project, materialized_view)
        assert len(indexes) == 1
        assert indexes[0]["column_names"] == "value"

    @staticmethod
    def change_config_via_replace(project, materialized_view):
        # dbt-postgres does not currently monitor changes of this type
        pass


class TestPostgresMaterializedViewChangesApply(
    PostgresMaterializedViewChanges, MaterializedViewChangesApplyMixin
):
    @pytest.mark.skip("dbt-postgres does not currently monitor replace changes.")
    def test_change_is_applied_via_replace(self, project, my_materialized_view):
        super().test_change_is_applied_via_replace(project, my_materialized_view)


class TestPostgresMaterializedViewChangesContinue(
    PostgresMaterializedViewChanges, MaterializedViewChangesContinueMixin
):
    @pytest.mark.skip("dbt-postgres does not currently monitor replace changes.")
    def test_change_is_not_applied_via_replace(self, project, my_materialized_view):
        super().test_change_is_not_applied_via_alter(project, my_materialized_view)


class TestPostgresMaterializedViewChangesFail(
    PostgresMaterializedViewChanges, MaterializedViewChangesFailMixin
):
    @pytest.mark.skip("dbt-postgres does not currently monitor replace changes.")
    def test_change_is_not_applied_via_replace(self, project, my_materialized_view):
        super().test_change_is_not_applied_via_replace(project, my_materialized_view)
