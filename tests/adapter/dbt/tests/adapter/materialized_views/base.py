from typing import List
import os
from dataclasses import dataclass

import pytest

from dbt.tests.util import run_dbt, get_manifest
from dbt.contracts.relation import RelationType


@dataclass
class Model:
    name: str
    definition: str
    columns: List[str] = None


class Base:

    base_materialized_view = Model(
        name="base_materialized_view",
        definition="{{ config(materialized='materialized_view') }} select * from {{ ref('base_table') }}",
    )

    base_table = Model(
        name="base_table",
        definition="{{ config(materialized='table') }} select 1 as base_column where 0 = 1",
        columns=["base_column"],
    )

    starting_records = [(1,)]
    inserted_records = [(2,)]

    @pytest.fixture(scope="class")
    def models(self):
        return {
            f"{model.name}.sql": model.definition
            for model in [self.base_table, self.base_materialized_view]
        }

    @pytest.fixture(scope="function", autouse=True)
    def setup(self, project):
        run_dbt(["run", "--models", self.base_table.name])
        self.insert_records(project, self.starting_records, self.base_table)
        run_dbt(["run", "--models", self.base_materialized_view.name])

    @staticmethod
    def get_records(project, model: Model) -> List[tuple]:
        sql = f"select * from {project.database}.{project.test_schema}.{model.name};"
        return [tuple(row) for row in project.run_sql(sql, fetch="all")]

    def insert_record(self, project, record: tuple, model: Model = None):
        model = model or self.base_table

        sql = f"""
        insert into {project.database}.{project.test_schema}.{model.name} ({', '.join(model.columns)})
        values ({','.join(str(value) for value in record)})
        ;"""
        project.run_sql(sql)

    def insert_records(self, project, records: List[tuple], model: Model = None):
        for record in records:
            self.insert_record(project, record, model)

    def assert_relation_is_materialized_view(self, project, model: Model):
        manifest = get_manifest(project.project_root)
        model = manifest.nodes[f"model.test.{model.name}"]
        assert model.config.materialized == RelationType.MaterializedView
        assert len(self.get_records(project, model)) >= 0

    @staticmethod
    def assert_message_in_logs(logs: str, message: str, expected_fail: bool = False):
        # if the logs are json strings, then 'jsonify' the message because of things like escape quotes
        if os.environ.get("DBT_LOG_FORMAT", "") == "json":
            message = message.replace(r'"', r"\"")

        if expected_fail:
            assert message not in logs
        else:
            assert message in logs
