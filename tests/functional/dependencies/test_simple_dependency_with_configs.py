import pytest

from pathlib import Path

from dbt.tests.util import (
    check_relations_equal,
    run_dbt,
)


models__view_summary = """
{{
    config(
        materialized='view'
    )
}}


with t as (

    select * from {{ ref('view_model') }}

)

select date_trunc('year', updated_at) as year,
       count(*)
from t
group by 1
"""


class BaseTestSimpleDependencyWithConfigs(object):
    @pytest.fixture(scope="class", autouse=True)
    def setUp(self, project):
        project.run_sql_file(project.test_data_dir / Path("seed.sql"))

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "view_summary.sql": models__view_summary,
        }


class TestSimpleDependencyWithConfigs(BaseTestSimpleDependencyWithConfigs):
    @pytest.fixture(scope="class")
    def packages(self):
        return {
            "packages": [
                {
                    "git": "https://github.com/dbt-labs/dbt-integration-project",
                    "revision": "with-configs-1.0.0",
                },
            ]
        }

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "config-version": 2,
            "vars": {
                "dbt_integration_project": {"bool_config": True},
            },
        }

    def test_simple_dependency(self, project):
        run_dbt(["deps"])
        results = run_dbt()
        assert len(results) == 5

        check_relations_equal(project.adapter, ["seed_config_expected_1", "config"])
        check_relations_equal(project.adapter, ["seed", "table_model"])
        check_relations_equal(project.adapter, ["seed", "view_model"])
        check_relations_equal(project.adapter, ["seed", "incremental"])


class TestSimpleDependencyWithOverriddenConfigs(BaseTestSimpleDependencyWithConfigs):
    @pytest.fixture(scope="class")
    def packages(self):
        return {
            "packages": [
                {
                    "git": "https://github.com/dbt-labs/dbt-integration-project",
                    "revision": "with-configs-1.0.0",
                },
            ]
        }

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "config-version": 2,
            "vars": {
                # project-level configs
                "dbt_integration_project": {
                    "config_1": "abc",
                    "config_2": "def",
                    "bool_config": True,
                },
            },
        }

    def test_simple_dependency(self, project):
        run_dbt(["deps"])
        results = run_dbt(["run"])
        len(results) == 5

        check_relations_equal(project.adapter, ["seed_config_expected_2", "config"])
        check_relations_equal(project.adapter, ["seed", "table_model"])
        check_relations_equal(project.adapter, ["seed", "view_model"])
        check_relations_equal(project.adapter, ["seed", "incremental"])
