import pytest

from dbt.exceptions import ParsingError
from dbt.tests.util import get_manifest, rm_file, run_dbt, write_file

schema_yml = """
models:
  - name: foo
    config:
      materialized: table
      contract:
        enforced: true
    constraints:
      - type: primary_key
        columns: [id, user_name]
    columns:
      - name: id
        data_type: int
        constraints:
          - type: not_null
      - name: user_name
        data_type: text
"""

foo_sql = """
select 1 as id, 'alice' as user_name
"""

foo_v2_sql = """
select 1 as id, 'alice' as user_name, 2 as another_pk
"""

versioned_schema_yml = """
models:
  - name: foo
    latest_version: 1
    config:
      materialized: table
      contract:
        enforced: true
    constraints:
      - type: primary_key
        columns: [id, user_name]
    columns:
      - name: id
        data_type: int
        constraints:
          - type: not_null
      - name: user_name
        data_type: text
    versions:
      - v: 1
"""

versioned_pk_model_column_schema_yml = """
models:
  - name: foo
    latest_version: 2
    config:
      materialized: table
      contract:
        enforced: true
    constraints:
      - type: primary_key
        columns: [id]
    columns:
      - name: id
        data_type: int
        constraints:
          - type: not_null
      - name: user_name
        data_type: text
    versions:
      - v: 1
      - v: 2
        columns:
          - name: id
            data_type: int
            constraints:
              - type: not_null
              - type: primary_key
          - name: user_name
            data_type: text
"""

versioned_pk_mult_columns_schema_yml = """
models:
  - name: foo
    latest_version: 2
    config:
      materialized: table
      contract:
        enforced: true
    columns:
      - name: id
        data_type: int
        constraints:
          - type: not_null
          - type: primary_key
      - name: user_name
        data_type: text
    versions:
      - v: 1
      - v: 2
        columns:
          - name: id
            data_type: int
            constraints:
              - type: not_null
              - type: primary_key
          - name: user_name
            data_type: text
            constraints:
              - type: primary_key

"""


class TestVersionedModelConstraints:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "foo.sql": foo_sql,
            "schema.yml": schema_yml,
        }

    def test_versioned_model_constraints(self, project):
        results = run_dbt(["run"])
        assert len(results) == 1
        manifest = get_manifest(project.project_root)
        model_node = manifest.nodes["model.test.foo"]
        assert len(model_node.constraints) == 1

        # remove foo.sql and create foo_v1.sql
        rm_file(project.project_root, "models", "foo.sql")
        write_file(foo_sql, project.project_root, "models", "foo_v1.sql")
        write_file(versioned_schema_yml, project.project_root, "models", "schema.yml")
        results = run_dbt(["run"])
        assert len(results) == 1

        manifest = get_manifest(project.project_root)
        model_node = manifest.nodes["model.test.foo.v1"]
        assert model_node.contract.enforced is True
        assert len(model_node.constraints) == 1


# test primary key defined across model and column level constraints, expect error
class TestPrimaryKeysModelAndColumnLevelConstraints:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "foo.sql": foo_sql,
            "schema.yml": schema_yml,
        }

    def test_model_column_pk_error(self, project):
        results = run_dbt(["run"])
        assert len(results) == 1
        manifest = get_manifest(project.project_root)
        model_node = manifest.nodes["model.test.foo"]
        assert len(model_node.constraints) == 1

        # remove foo.sql and create foo_v1.sql
        rm_file(project.project_root, "models", "foo.sql")
        write_file(foo_sql, project.project_root, "models", "foo_v1.sql")
        write_file(versioned_schema_yml, project.project_root, "models", "schema.yml")
        results = run_dbt(["run"])
        assert len(results) == 1

        manifest = get_manifest(project.project_root)
        model_node = manifest.nodes["model.test.foo.v1"]
        assert model_node.contract.enforced is True
        assert len(model_node.constraints) == 1

        # add foo_v2.sql
        write_file(foo_sql, project.project_root, "models", "foo_v2.sql")
        write_file(
            versioned_pk_model_column_schema_yml, project.project_root, "models", "schema.yml"
        )

        expected_error = "Primary key constraints defined at the model level and the columns level"
        with pytest.raises(ParsingError) as exc_info:
            run_dbt(["run"])
        assert expected_error in str(exc_info.value)


# test primary key defined across multiple columns, expect error
class TestPrimaryKeysMultipleColumns:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "foo.sql": foo_sql,
            "schema.yml": schema_yml,
        }

    def test_pk_multiple_columns(self, project):
        results = run_dbt(["run"])
        assert len(results) == 1
        manifest = get_manifest(project.project_root)
        model_node = manifest.nodes["model.test.foo"]
        assert len(model_node.constraints) == 1

        # remove foo.sql and create foo_v1.sql
        rm_file(project.project_root, "models", "foo.sql")
        write_file(foo_sql, project.project_root, "models", "foo_v1.sql")
        write_file(versioned_schema_yml, project.project_root, "models", "schema.yml")
        results = run_dbt(["run"])
        assert len(results) == 1

        manifest = get_manifest(project.project_root)
        model_node = manifest.nodes["model.test.foo.v1"]
        assert model_node.contract.enforced is True
        assert len(model_node.constraints) == 1

        # add foo_v2.sql
        write_file(foo_sql, project.project_root, "models", "foo_v2.sql")
        write_file(
            versioned_pk_mult_columns_schema_yml, project.project_root, "models", "schema.yml"
        )

        expected_error = (
            "Found 2 columns (['id', 'user_name']) with primary key constraints defined"
        )
        with pytest.raises(ParsingError) as exc_info:
            run_dbt(["run"])
        assert expected_error in str(exc_info.value)
