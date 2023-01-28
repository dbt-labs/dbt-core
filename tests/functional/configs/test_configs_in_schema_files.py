import pytest

from dbt.tests.util import run_dbt, get_manifest, check_relations_equal, write_file

from dbt.exceptions import CompilationError, ParsingError

models_alt__schema_yml = """
version: 2
sources:
  - name: raw
    database: "{{ target.database }}"
    schema: "{{ target.schema }}"
    tables:
      - name: 'some_seed'
        columns:
          - name: id

models:
  - name: model
    description: "This is a model description"
    config:
        tags: ['tag_in_schema']
        meta:
            owner: 'Julie Smith'
            my_attr: "{{ var('my_var') }}"
        materialized: view

    columns:
      - name: id
        tests:
          - not_null:
              meta:
                  owner: 'Simple Simon'
          - unique:
              config:
                  meta:
                      owner: 'John Doe'
"""

models_alt__untagged_sql = """
{{
    config(materialized='table')
}}

select id, value from {{ source('raw', 'some_seed') }}
"""

models_alt__tagged__model_sql = """
{{
    config(
        materialized='view',
        tags=['tag_1_in_model'],
    )
}}

{{
    config(
        materialized='table',
        tags=['tag_2_in_model'],
    )
}}

select 4 as id, 2 as value
"""

models_no_materialized__model_sql = """
{{
    config(
        tags=['tag_1_in_model'],
    )
}}

{{
    config(
        tags=['tag_2_in_model'],
    )
}}

select 4 as id, 2 as value
"""

seeds_alt__some_seed_csv = """id,value
4,2
"""

extra_alt__untagged_yml = """
version: 2

models:
  - name: untagged
    description: "This is a model description"
    meta:
      owner: 'Somebody Else'
    config:
        meta:
            owner: 'Julie Smith'
"""

extra_alt__untagged2_yml = """
version: 2

models:
  - name: untagged
    description: "This is a model description"
    tests:
      - not_null:
          error_if: ">2"
          config:
            error_if: ">2"
"""


class TestSchemaFileConfigs:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "schema.yml": models_alt__schema_yml,
            "untagged.sql": models_alt__untagged_sql,
            "tagged": {"model.sql": models_alt__tagged__model_sql},
        }

    @pytest.fixture(scope="class")
    def seeds(self):
        return {"some_seed.csv": seeds_alt__some_seed_csv}

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "models": {
                "+meta": {
                    "company": "NuMade",
                },
                "test": {
                    "+meta": {
                        "project": "test",
                    },
                    "tagged": {
                        "+meta": {
                            "team": "Core Team",
                        },
                        "tags": ["tag_in_project"],
                        "model": {
                            "materialized": "table",
                            "+meta": {
                                "owner": "Julie Dent",
                            },
                        },
                    },
                },
            },
            "vars": {
                "test": {
                    "my_var": "TESTING",
                }
            },
            "seeds": {
                "quote_columns": False,
            },
        }

    def test_config_layering(
        self,
        project,
    ):

        # run seed
        assert len(run_dbt(["seed"])) == 1

        # test the project-level tag, and both config() call tags
        assert len(run_dbt(["run", "--model", "tag:tag_in_project"])) == 1
        assert len(run_dbt(["run", "--model", "tag:tag_1_in_model"])) == 1
        assert len(run_dbt(["run", "--model", "tag:tag_2_in_model"])) == 1
        assert len(run_dbt(["run", "--model", "tag:tag_in_schema"])) == 1

        # Verify that model nodes have expected tags and meta
        manifest = get_manifest(project.project_root)
        model_id = "model.test.model"
        model_node = manifest.nodes[model_id]
        meta_expected = {
            "company": "NuMade",
            "project": "test",
            "team": "Core Team",
            "owner": "Julie Smith",
            "my_attr": "TESTING",
        }
        assert model_node.meta == meta_expected
        assert model_node.config.meta == meta_expected
        model_tags = ["tag_1_in_model", "tag_2_in_model", "tag_in_project", "tag_in_schema"]
        model_node_tags = model_node.tags.copy()
        model_node_tags.sort()
        assert model_node_tags == model_tags
        model_node_config_tags = model_node.config.tags.copy()
        model_node_config_tags.sort()
        assert model_node_config_tags == model_tags
        model_meta = {
            "company": "NuMade",
            "project": "test",
            "team": "Core Team",
            "owner": "Julie Smith",
            "my_attr": "TESTING",
        }
        assert model_node.config.meta == model_meta

        # make sure we overwrote the materialization properly
        tables = project.get_tables_in_schema()
        assert tables["model"] == "table"
        check_relations_equal(project.adapter, ["some_seed", "model"])

        # Remove materialized config from model
        write_file(
            models_no_materialized__model_sql,
            project.project_root,
            "models",
            "tagged",
            "model.sql",
        )
        results = run_dbt(["run"])
        assert len(results) == 2
        manifest = get_manifest(project.project_root)
        model_node = manifest.nodes[model_id]

        assert model_node.config.materialized == "view"
        model_unrendered_config = {
            "materialized": "view",
            "meta": {"my_attr": "TESTING", "owner": "Julie Smith"},
            "tags": ["tag_1_in_model", "tag_2_in_model"],
        }
        assert model_node.unrendered_config == model_unrendered_config

        # look for test meta
        schema_file_id = model_node.patch_path
        schema_file = manifest.files[schema_file_id]
        tests = schema_file.get_tests("models", "model")
        assert tests[0] in manifest.nodes
        test = manifest.nodes[tests[0]]
        expected_meta = {"owner": "Simple Simon"}
        assert test.config.meta == expected_meta
        test = manifest.nodes[tests[1]]
        expected_meta = {"owner": "John Doe"}
        assert test.config.meta == expected_meta

        # copy a schema file with multiple metas
        #       shutil.copyfile('extra-alt/untagged.yml', 'models-alt/untagged.yml')
        write_file(extra_alt__untagged_yml, project.project_root, "models", "untagged.yml")
        with pytest.raises(ParsingError):
            run_dbt(["run"])

        # copy a schema file with config key in top-level of test and in config dict
        #       shutil.copyfile('extra-alt/untagged2.yml', 'models-alt/untagged.yml')
        write_file(extra_alt__untagged2_yml, project.project_root, "models", "untagged.yml")
        with pytest.raises(CompilationError):
            run_dbt(["run"])
