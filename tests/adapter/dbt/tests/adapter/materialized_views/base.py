from typing import List
import os

import pytest

from dbt.tests.util import run_dbt, get_manifest
from dbt.contracts.relation import RelationType


class Base:

    materialized_view = "mat_view"
    base_table = "base_table"
    base_table_columns = ["base_column"]
    starting_records = [(1,)]
    inserted_records = [(2,)]

    @pytest.fixture(scope="class")
    def models(self):
        base_table = "{{ config(materialized='table') }} select 1 as base_column where 0 = 1"
        mat_view = (
            "{{ config(materialized='materialized_view') }} select * from {{ ref('base_table') }}"
        )
        return {
            "base_table.sql": base_table,
            "mat_view.sql": mat_view,
        }

    @pytest.fixture(scope="function", autouse=True)
    def setup(self, project):
        run_dbt(["run", "--models", self.base_table])
        self.insert_records(
            project, self.starting_records, self.base_table, self.base_table_columns
        )
        run_dbt(["run", "--models", self.materialized_view])

    @staticmethod
    def get_records(project, relation_identifier: str) -> List[tuple]:
        sql = f"select * from {project.database}.{project.test_schema}.{relation_identifier};"
        return [tuple(row) for row in project.run_sql(sql, fetch="all")]

    def insert_record(
        self, project, record: tuple, relation_identifier: str = None, columns: List[str] = None
    ):
        relation_identifier = relation_identifier or self.base_table
        columns = columns or self.base_table_columns

        sql = f"""
        insert into {project.database}.{project.test_schema}.{relation_identifier} ({', '.join(columns)})
        values ({','.join(str(value) for value in record)})
        ;"""
        project.run_sql(sql)

    def insert_records(
        self,
        project,
        records: List[tuple],
        relation_identifier: str = None,
        columns: List[str] = None,
    ):
        for record in records:
            self.insert_record(project, record, relation_identifier, columns)

    def assert_relation_is_materialized_view(self, project, relation_identifier):
        manifest = get_manifest(project.project_root)
        model = manifest.nodes[f"model.test.{relation_identifier}"]
        assert model.config.materialized == RelationType.MaterializedView
        assert len(self.get_records(project, relation_identifier)) >= 0

    @staticmethod
    def assert_message_in_logs(logs: str, message: str, expected_fail: bool = False):
        # if the logs are json strings, then 'jsonify' the message because of things like escape quotes
        if os.environ.get("DBT_LOG_FORMAT", "") == "json":
            message = message.replace(r'"', r"\"")

        if expected_fail:
            assert message not in logs
        else:
            assert message in logs
