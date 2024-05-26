import pytest

from dbt.exceptions import ParsingError
from dbt.tests.util import get_manifest, run_dbt, write_file
from tests.functional.partial_parsing.fixtures import (
    groups_schema_yml_one_group,
    groups_schema_yml_one_group_model_in_group2,
    groups_schema_yml_two_groups,
    groups_schema_yml_two_groups_edited,
    groups_schema_yml_two_groups_private_orders_invalid_access,
    groups_schema_yml_two_groups_private_orders_valid_access,
    orders_downstream_sql,
    orders_sql,
)


class TestGroups:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "orders.sql": orders_sql,
            "orders_downstream.sql": orders_downstream_sql,
            "schema.yml": groups_schema_yml_one_group,
        }

    def test_pp_groups(self, project):

        # initial run
        results = run_dbt()
        assert len(results) == 2
        manifest = get_manifest(project.project_root)
        expected_nodes = ["model.test.orders", "model.test.orders_downstream"]
        expected_groups = ["group.test.test_group"]
        assert expected_nodes == sorted(list(manifest.nodes.keys()))
        assert expected_groups == sorted(list(manifest.groups.keys()))

        # add group to schema
        write_file(groups_schema_yml_two_groups, project.project_root, "models", "schema.yml")
        results = run_dbt(["--partial-parse", "run"])
        assert len(results) == 2
        manifest = get_manifest(project.project_root)
        expected_nodes = ["model.test.orders", "model.test.orders_downstream"]
        expected_groups = ["group.test.test_group", "group.test.test_group2"]
        assert expected_nodes == sorted(list(manifest.nodes.keys()))
        assert expected_groups == sorted(list(manifest.groups.keys()))

        # edit group in schema
        write_file(
            groups_schema_yml_two_groups_edited, project.project_root, "models", "schema.yml"
        )
        results = run_dbt(["--partial-parse", "run"])
        assert len(results) == 2
        manifest = get_manifest(project.project_root)
        expected_nodes = ["model.test.orders", "model.test.orders_downstream"]
        expected_groups = ["group.test.test_group", "group.test.test_group2_edited"]
        assert expected_nodes == sorted(list(manifest.nodes.keys()))
        assert expected_groups == sorted(list(manifest.groups.keys()))

        # delete group in schema
        write_file(groups_schema_yml_one_group, project.project_root, "models", "schema.yml")
        results = run_dbt(["--partial-parse", "run"])
        assert len(results) == 2
        manifest = get_manifest(project.project_root)
        expected_nodes = ["model.test.orders", "model.test.orders_downstream"]
        expected_groups = ["group.test.test_group"]
        assert expected_nodes == sorted(list(manifest.nodes.keys()))
        assert expected_groups == sorted(list(manifest.groups.keys()))

        # add back second group
        write_file(groups_schema_yml_two_groups, project.project_root, "models", "schema.yml")
        results = run_dbt(["--partial-parse", "run"])
        assert len(results) == 2

        # remove second group with model still configured to second group
        write_file(
            groups_schema_yml_one_group_model_in_group2,
            project.project_root,
            "models",
            "schema.yml",
        )
        with pytest.raises(ParsingError):
            results = run_dbt(["--partial-parse", "run"])

        # add back second group, make orders private with valid ref
        write_file(
            groups_schema_yml_two_groups_private_orders_valid_access,
            project.project_root,
            "models",
            "schema.yml",
        )
        results = run_dbt(["--partial-parse", "run"])
        assert len(results) == 2

        write_file(
            groups_schema_yml_two_groups_private_orders_invalid_access,
            project.project_root,
            "models",
            "schema.yml",
        )
        with pytest.raises(ParsingError):
            results = run_dbt(["--partial-parse", "run"])


my_model_c = """
select * from {{ ref("my_model_a") }} union all
select * from {{ ref("my_model_b") }}
"""

models_yml = """
models:
  - name: my_model_a
  - name: my_model_b
  - name: my_model_c
"""

models_and_groups_yml = """
groups:
  - name: sales_analytics
    owner:
      name: Sales Analytics
      email: sales@jaffleshop.com

models:
  - name: my_model_a
    access: private
    group: sales_analytics
  - name: my_model_b
    access: private
    group: sales_analytics
  - name: my_model_c
    access: private
    group: sales_analytics
"""


class TestAddingModelsToNewGroups:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model_a.sql": "select 1 as id",
            "my_model_b.sql": "select 2 as id",
            "my_model_c.sql": my_model_c,
            "models.yml": models_yml,
        }

    def test_adding_models_to_new_groups(self, project):
        run_dbt(["compile"])
        # This tests that the correct patch is added to my_model_c. The bug
        # was that it was using the old patch, so model_c didn't have the
        # correct group and access.
        write_file(models_and_groups_yml, project.project_root, "models", "models.yml")
        run_dbt(["compile"])
        manifest = get_manifest(project.project_root)
        model_c_node = manifest.nodes["model.test.my_model_c"]
        assert model_c_node.group == "sales_analytics"
        assert model_c_node.access == "private"
