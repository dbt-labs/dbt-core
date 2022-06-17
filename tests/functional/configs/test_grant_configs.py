import pytest

from dbt.tests.util import run_dbt, get_manifest, write_file, write_config_file

dbt_project_yml = """
models:
  test:
    my_model:
      +grants:
        my_select: ["reporter", "bi"]
"""

append_schema_yml = """
version: 2
models:
  - name: my_model
    config:
      grants:
        +my_select: ["someone"]
"""


my_model_base_sql = """
select 1 as fun
"""


my_model_clobber_sql = """
{{ config(grants={'my_select': ['other_user']}) }}
select 1 as fun
"""

my_model_extend_sql = """
{{ config(grants={'+my_select': ['other_user']}) }}
select 1 as fun
"""

my_model_extend_string_sql = """
{{ config(grants={'+my_select': 'other_user'}) }}
select 1 as fun
"""

my_model_extend_twice_sql = """
{{ config(grants={'+my_select': ['other_user']}) }}
{{ config(grants={'+my_select': ['alt_user']}) }}
select 1 as fun
"""


class TestGrantConfigs:
    @pytest.fixture(scope="class")
    def models(self):
        return {"my_model.sql": my_model_base_sql}

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return dbt_project_yml

    def test_model_grant_config(self, project, logs_dir):
        # This test uses "my_select" instead of "select", so that when
        # actual granting of permissions happens, it won't break this
        # test.
        results = run_dbt(["run"])
        assert len(results) == 1

        manifest = get_manifest(project.project_root)
        model_id = "model.test.my_model"
        assert model_id in manifest.nodes

        model = manifest.nodes[model_id]
        model_config = model.config
        assert hasattr(model_config, "grants")

        # no schema grant, no model grant, just project
        expected = {"my_select": ["reporter", "bi"]}
        assert model_config.grants == expected

        # add model grant with clobber
        write_file(my_model_clobber_sql, project.project_root, "models", "my_model.sql")
        results = run_dbt(["run"])
        manifest = get_manifest(project.project_root)
        model_config = manifest.nodes[model_id].config

        expected = {"my_select": ["other_user"]}
        assert model_config.grants == expected

        # change model to extend grants
        write_file(my_model_extend_sql, project.project_root, "models", "my_model.sql")
        results = run_dbt(["run"])
        manifest = get_manifest(project.project_root)
        model_config = manifest.nodes[model_id].config

        expected = {"my_select": ["reporter", "bi", "other_user"]}
        assert model_config.grants == expected

        # add schema file with extend
        write_file(append_schema_yml, project.project_root, "models", "schema.yml")
        results = run_dbt(["run"])
        assert len(results) == 1

        manifest = get_manifest(project.project_root)
        model_config = manifest.nodes[model_id].config

        expected = {"my_select": ["reporter", "bi", "someone", "other_user"]}
        assert model_config.grants == expected

        # change model file to have string instead of list
        write_file(my_model_extend_string_sql, project.project_root, "models", "my_model.sql")
        results = run_dbt(["run"])
        assert len(results) == 1

        manifest = get_manifest(project.project_root)
        model_config = manifest.nodes[model_id].config

        expected = {"my_select": ["reporter", "bi", "someone", "other_user"]}
        assert model_config.grants == expected

        # change model file to have string instead of list
        write_file(my_model_extend_twice_sql, project.project_root, "models", "my_model.sql")
        results = run_dbt(["run"])
        assert len(results) == 1

        manifest = get_manifest(project.project_root)
        model_config = manifest.nodes[model_id].config

        expected = {"my_select": ["reporter", "bi", "someone", "other_user", "alt_user"]}
        assert model_config.grants == expected

        # Remove grant from dbt_project
        config = {
            "config-version": 2,
            "name": "test",
            "version": "0.1.0",
            "profile": "test",
            "log-path": logs_dir,
        }
        write_config_file(config, project.project_root, "dbt_project.yml")
        results = run_dbt(["run"])
        assert len(results) == 1

        manifest = get_manifest(project.project_root)
        model_config = manifest.nodes[model_id].config

        expected = {"my_select": ["someone", "other_user", "alt_user"]}
        assert model_config.grants == expected

        # Remove my_model config, leaving only schema file
        write_file(my_model_base_sql, project.project_root, "models", "my_model.sql")
        results = run_dbt(["run"])
        assert len(results) == 1

        manifest = get_manifest(project.project_root)
        model_config = manifest.nodes[model_id].config

        expected = {"my_select": ["someone"]}
        assert model_config.grants == expected
