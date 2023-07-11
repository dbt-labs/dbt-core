import pytest
import os
from dbt.exceptions import ParsingError, ValidationError
from dbt.tests.util import run_dbt, get_manifest, get_artifact, run_dbt_and_capture, write_file

my_model_sql = """
{{
  config(
    materialized = "table"
  )
}}

select
  'blue' as color,
  1 as id,
  cast('2019-01-01' as date) as date_day
"""

my_model_contract_sql = """
{{
  config(
    materialized = "table",
    contract = {"enforced": true}
  )
}}

select
  1 as id,
  'blue' as color,
  cast('2019-01-01' as date) as date_day
"""

my_model_contract_disabled_sql = """
{{
  config(
    materialized = "table",
    contract = {"enforced": false}
  )
}}

select
  1 as id,
  'blue' as color,
  cast('2019-01-01' as date) as date_day
"""

my_incremental_model_sql = """
{{
  config(
    materialized = "incremental"
  )
}}

select
  1 as id,
  'blue' as color,
  cast('2019-01-01' as date) as date_day
"""

my_view_model_sql = """
{{
  config(
    materialized = "view"
  )
}}

select
  1 as id,
  'blue' as color,
  cast('2019-01-01' as date) as date_day
"""

my_model_python_error = """
import holidays, s3fs


def model(dbt, _):
    dbt.config(
        materialized="table",
        packages=["holidays", "s3fs"],  # how to import python libraries in dbt's context
    )
    df = dbt.ref("my_model")
    df_describe = df.describe()  # basic statistics profiling
    return df_describe
"""

model_schema_yml = """
version: 2
models:
  - name: my_model
    config:
      contract:
        enforced: true
    columns:
      - name: id
        quote: true
        data_type: integer
        description: hello
        constraints:
            - type: not_null
            - type: primary_key
            - type: check
              expression: (id > 0)
        tests:
          - unique
      - name: color
        data_type: text
      - name: date_day
        data_type: date
"""

model_schema_ignore_unsupported_yml = """
version: 2
models:
  - name: my_model
    config:
      contract:
        enforced: true
    columns:
      - name: id
        quote: true
        data_type: integer
        description: hello
        constraints:
            - type: not_null
              warn_unsupported: False
            - type: primary_key
              warn_unsupported: False
            - type: check
              warn_unsupported: False
              expression: (id > 0)
        tests:
          - unique
      - name: color
        data_type: text
      - name: date_day
        data_type: date
"""

model_schema_errors_yml = """
version: 2
models:
  - name: my_model
    config:
      contract:
        enforced: true
    columns:
      - name: id
        data_type: integer
        description: hello
        constraints:
            - type: not_null
            - type: primary_key
            - type: check
              expression: (id > 0)
        tests:
          - unique
      - name: color
        data_type: text
      - name: date_day
  - name: python_model
    config:
      contract:
        enforced: true
    columns:
      - name: id
        data_type: integer
        description: hello
        constraints:
            - type: not_null
            - type: primary_key
            - type: check
              expression: (id > 0)
        tests:
          - unique
      - name: color
        data_type: text
      - name: date_day
        data_type: date
"""

model_schema_blank_yml = """
version: 2
models:
  - name: my_model
    config:
      contract:
        enforced: true
"""

model_schema_complete_datatypes_yml = """
version: 2
models:
  - name: my_model
    columns:
      - name: id
        quote: true
        data_type: integer
        description: hello
        constraints:
          - type: not_null
          - type: primary_key
          - type: check
            expression: (id > 0)
        tests:
          - unique
      - name: color
        data_type: text
      - name: date_day
        data_type: date
"""

model_schema_incomplete_datatypes_yml = """
version: 2
models:
  - name: my_model
    columns:
      - name: id
        quote: true
        data_type: integer
        description: hello
        constraints:
          - type: not_null
          - type: primary_key
          - type: check
            expression: (id > 0)
        tests:
          - unique
      - name: color
      - name: date_day
        data_type: date
"""


class TestModelLevelContractEnabledConfigs:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model.sql": my_model_sql,
            "constraints_schema.yml": model_schema_yml,
        }

    def test__model_contract_true(self, project):
        run_dbt(["run"])
        manifest = get_manifest(project.project_root)
        model_id = "model.test.my_model"
        model = manifest.nodes[model_id]
        my_model_columns = model.columns
        my_model_config = model.config
        contract_actual_config = my_model_config.contract

        assert contract_actual_config.enforced is True

        expected_columns = "{'id': ColumnInfo(name='id', description='hello', meta={}, data_type='integer', constraints=[ColumnLevelConstraint(type=<ConstraintType.not_null: 'not_null'>, name=None, expression=None, warn_unenforced=True, warn_unsupported=True), ColumnLevelConstraint(type=<ConstraintType.primary_key: 'primary_key'>, name=None, expression=None, warn_unenforced=True, warn_unsupported=True), ColumnLevelConstraint(type=<ConstraintType.check: 'check'>, name=None, expression='(id > 0)', warn_unenforced=True, warn_unsupported=True)], quote=True, tags=[], _extra={}), 'color': ColumnInfo(name='color', description='', meta={}, data_type='text', constraints=[], quote=None, tags=[], _extra={}), 'date_day': ColumnInfo(name='date_day', description='', meta={}, data_type='date', constraints=[], quote=None, tags=[], _extra={})}"

        assert expected_columns == str(my_model_columns)

        # compiled fields aren't in the manifest above because it only has parsed fields
        manifest_json = get_artifact(project.project_root, "target", "manifest.json")
        compiled_code = manifest_json["nodes"][model_id]["compiled_code"]
        cleaned_code = " ".join(compiled_code.split())
        assert (
            "select 'blue' as color, 1 as id, cast('2019-01-01' as date) as date_day"
            == cleaned_code
        )


class TestProjectContractEnabledConfigs:
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"models": {"test": {"+contract": {"enforced": True}}}}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model.sql": my_model_sql,
            "constraints_schema.yml": model_schema_complete_datatypes_yml,
        }

    def test_defined_column_type(self, project):
        run_dbt(["run"], expect_pass=True)
        manifest = get_manifest(project.project_root)
        model_id = "model.test.my_model"
        my_model_config = manifest.nodes[model_id].config
        contract_actual_config = my_model_config.contract
        assert contract_actual_config.enforced is True


class TestProjectContractEnabledConfigsError:
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "models": {
                "test": {
                    "+contract": {
                        "enforced": True,
                    },
                }
            }
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model.sql": my_model_sql,
            "constraints_schema.yml": model_schema_incomplete_datatypes_yml,
        }

    def test_undefined_column_type(self, project):
        _, log_output = run_dbt_and_capture(["run", "-s", "my_model"], expect_pass=False)
        manifest = get_manifest(project.project_root)
        model_id = "model.test.my_model"
        my_model_config = manifest.nodes[model_id].config
        contract_actual_config = my_model_config.contract

        assert contract_actual_config.enforced is True

        expected_compile_error = "Please ensure that the column name and data_type are defined within the YAML configuration for the ['color'] column(s)."

        assert expected_compile_error in log_output


class TestModelContractEnabledConfigs:
    @pytest.fixture(scope="class")
    def models(self):
        return {"my_model.sql": my_model_contract_sql, "constraints_schema.yml": model_schema_yml}

    def test__model_contract(self, project):
        run_dbt(["run"])
        manifest = get_manifest(project.project_root)
        model_id = "model.test.my_model"
        my_model_config = manifest.nodes[model_id].config
        contract_actual_config = my_model_config.contract
        assert contract_actual_config.enforced is True


class TestModelContractEnabledConfigsMissingDataTypes:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model.sql": my_model_contract_sql,
            "constraints_schema.yml": model_schema_incomplete_datatypes_yml,
        }

    def test_undefined_column_type(self, project):
        _, log_output = run_dbt_and_capture(["run", "-s", "my_model"], expect_pass=False)
        manifest = get_manifest(project.project_root)
        model_id = "model.test.my_model"
        my_model_config = manifest.nodes[model_id].config
        contract_actual_config = my_model_config.contract

        assert contract_actual_config.enforced is True

        expected_compile_error = "Please ensure that the column name and data_type are defined within the YAML configuration for the ['color'] column(s)."

        assert expected_compile_error in log_output


class TestModelLevelContractDisabledConfigs:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model.sql": my_model_contract_disabled_sql,
            "constraints_schema.yml": model_schema_yml,
        }

    def test__model_contract_false(self, project):

        run_dbt(["parse"])
        manifest = get_manifest(project.project_root)
        model_id = "model.test.my_model"
        my_model_config = manifest.nodes[model_id].config
        contract_actual_config = my_model_config.contract

        assert contract_actual_config.enforced is False


class TestModelLevelContractErrorMessages:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model.sql": my_incremental_model_sql,
            "constraints_schema.yml": model_schema_yml,
        }

    def test__config_errors(self, project):
        with pytest.raises(ValidationError) as err_info:
            run_dbt(["run"], expect_pass=False)

        exc_str = " ".join(str(err_info.value).split())
        expected_materialization_error = "Invalid value for on_schema_change: ignore. Models materialized as incremental with contracts enabled must set on_schema_change to 'append_new_columns' or 'fail'"
        assert expected_materialization_error in str(exc_str)


class TestModelLevelConstraintsErrorMessages:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model.py": my_model_python_error,
            "constraints_schema.yml": model_schema_yml,
        }

    def test__config_errors(self, project):
        with pytest.raises(ParsingError) as err_info:
            run_dbt(["run"], expect_pass=False)

        exc_str = " ".join(str(err_info.value).split())
        expected_materialization_error = "Language Error: Expected 'sql' but found 'python'"
        assert expected_materialization_error in str(exc_str)
        # This is a compile time error and we won't get here because the materialization check is parse time
        expected_empty_data_type_error = "Columns with `data_type` Blank/Null not allowed on contracted models. Columns Blank/Null: ['date_day']"
        assert expected_empty_data_type_error not in str(exc_str)


class TestModelLevelConstraintsWarningMessages:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model.sql": my_view_model_sql,
            "constraints_schema.yml": model_schema_yml,
        }

    def test__config_warning(self, project):
        _, log_output = run_dbt_and_capture(["run"])

        expected_materialization_warning = (
            "Constraint types are not supported for view materializations"
        )
        assert expected_materialization_warning in str(log_output)

        # change to not show warnings, message should not be in logs
        models_dir = os.path.join(project.project_root, "models")
        write_file(model_schema_ignore_unsupported_yml, models_dir, "constraints_schema.yml")
        _, log_output = run_dbt_and_capture(["run"])

        expected_materialization_warning = (
            "Constraint types are not supported for view materializations"
        )
        assert expected_materialization_warning not in str(log_output)


class TestSchemaContractEnabledConfigs:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model.sql": my_model_sql,
            "constraints_schema.yml": model_schema_blank_yml,
        }

    def test__schema_error(self, project):
        with pytest.raises(ParsingError) as err_info:
            run_dbt(["parse"], expect_pass=False)

        exc_str = " ".join(str(err_info.value).split())
        schema_error_expected = "Constraints must be defined in a `yml` schema configuration file"
        assert schema_error_expected in str(exc_str)


class TestPythonModelLevelContractErrorMessages:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "python_model.py": my_model_python_error,
            "constraints_schema.yml": model_schema_errors_yml,
        }

    def test__python_errors(self, project):
        with pytest.raises(ParsingError) as err_info:
            run_dbt(["parse"], expect_pass=False)

        exc_str = " ".join(str(err_info.value).split())
        expected_python_error = "Language Error: Expected 'sql' but found 'python'"
        assert expected_python_error in exc_str


class TestModelContractMissingYAMLColumns:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model.sql": my_model_contract_sql,
        }

    def test__missing_column_contract_error(self, project):
        results = run_dbt(["run"], expect_pass=False)
        expected_error = (
            "This model has an enforced contract, and its 'columns' specification is missing"
        )
        assert expected_error in results[0].message
