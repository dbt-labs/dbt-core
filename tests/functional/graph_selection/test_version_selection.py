import pytest

from dbt.tests.util import run_dbt, read_file
from tests.functional.graph_selection.fixtures import (
    schema_yml,
    base_users_sql,
    users_sql,
    users_rollup_sql,
    properties_yml,
)


selectors_yml = """
            selectors:
              - name: version_specified_as_string_str
                definition: version:latest
              - name: version_specified_as_string_dict
                definition:
                  method: version
                  value: latest
              - name: version_childrens_parents
                definition:
                  method: version
                  value: latest
                  childrens_parents: true
"""


class TestVersionSelection:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "schema.yml": schema_yml,
            "versioned_v1.sql": users_sql,
            "versioned_v2.sql": users_sql,
            "versioned_v3.sql": users_sql,
            "versioned_v4.5.sql": users_sql,
            "versioned_v5.0.sql": users_sql,
            "versioned_v21.sql": users_sql,
            "versioned_vtest.sql": users_sql,
            "base_users.sql": base_users_sql,
            "users.sql": users_sql,
            "users_rollup.sql": users_rollup_sql,
        }

    @pytest.fixture(scope="class")
    def seeds(self, test_data_dir):
        # Read seed file and return
        seeds = {"properties.yml": properties_yml}
        for seed_file in ["seed.csv", "summary_expected.csv"]:
            seeds[seed_file] = read_file(test_data_dir, seed_file)
        return seeds

    @pytest.fixture(scope="class")
    def selectors(self):
        return selectors_yml

    def test_select_none_versions(self, project):
        results = run_dbt(["ls", "--select", "version:none"])
        assert sorted(results) == [
            "test.base_users",
            "test.unique_users_id",
            "test.unique_users_rollup_gender",
            "test.users",
            "test.users_rollup",
        ]

    def test_select_latest_versions(self, project):
        results = run_dbt(["ls", "--select", "version:latest"])
        assert sorted(results) == ["test.versioned.v2"]

    def test_select_old_versions(self, project):
        results = run_dbt(["ls", "--select", "version:old"])
        assert sorted(results) == ["test.versioned.v1"]

    def test_select_prerelease_versions(self, project):
        results = run_dbt(["ls", "--select", "version:prerelease"])
        assert sorted(results) == [
            "test.versioned.v21",
            "test.versioned.v3",
            "test.versioned.v4.5",
            "test.versioned.v5.0",
            "test.versioned.vtest",
        ]

    def test_select_version_selector_str(self, project):
        results = run_dbt(["ls", "--selector", "version_specified_as_string_str"])
        assert sorted(results) == ["test.versioned.v2"]

    def test_select_version_selector_dict(self, project):
        results = run_dbt(["ls", "--selector", "version_specified_as_string_dict"])
        assert sorted(results) == ["test.versioned.v2"]

    def test_select_models_by_version_and_children(self, project):  # noqa
        results = run_dbt(["ls", "--models", "+version:latest+"])
        assert sorted(results) == ["test.base_users", "test.versioned.v2"]

    def test_select_version_and_children(self, project):  # noqa
        expected = ["source:test.raw.seed", "test.base_users", "test.versioned.v2"]
        results = run_dbt(["ls", "--select", "+version:latest+"])
        assert sorted(results) == expected

    def test_select_group_and_children_selector_str(self, project):  # noqa
        expected = ["source:test.raw.seed", "test.base_users", "test.versioned.v2"]
        results = run_dbt(["ls", "--selector", "version_childrens_parents"])
        assert sorted(results) == expected

    # 2 versions
    def test_select_models_two_versions(self, project):
        results = run_dbt(["ls", "--models", "version:latest version:old"])
        assert sorted(results) == ["test.versioned.v1", "test.versioned.v2"]


my_model_yml = """
models:
  - name: my_model
    versions:
      - v: 0
"""


class TestVersionZero:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model.sql": "select 1 as id",
            "another.sql": "select * from {{ ref('my_model') }}",
            "schema.yml": my_model_yml,
        }

    def test_version_zero(self, project):
        results = run_dbt(["run"])
        assert len(results) == 2
