import pytest
from dbt.exceptions import ParsingError
from dbt.tests.util import run_dbt, get_manifest

my_model_sql = """
{{
  config(
    materialized = "table"
  )
}}

select
  1 as id,
  'blue' as color,
  cast('2019-01-01' as date) as date_day
"""

my_model_contract_sql = """
{{
  config(
    materialized = "table",
    contract = true
  )
}}

select
  1 as id,
  'blue' as color,
  cast('2019-01-01' as date) as date_day
"""

my_model_constraints_disabled_sql = """
{{
  config(
    materialized = "table",
    contract = false
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
      contract: true
    columns:
      - name: id
        quote: true
        data_type: integer
        description: hello
        constraints: ['not null','primary key']
        constraints_check: (id > 0)
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
      contract: true
    columns:
      - name: id
        data_type: integer
        description: hello
        constraints: ['not null','primary key']
        constraints_check: (id > 0)
        tests:
          - unique
      - name: color
        data_type: text
      - name: date_day
  - name: python_model
    config:
      contract: true
    columns:
      - name: id
        data_type: integer
        description: hello
        constraints: ['not null','primary key']
        constraints_check: (id > 0)
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
      contract: true
"""


class TestModelLevelConstraintsEnabledConfigs:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model.sql": my_model_sql,
            "constraints_schema.yml": model_schema_yml,
        }

    def test__model_contract_true(self, project):
        run_dbt(["parse"])
        manifest = get_manifest(project.project_root)
        model_id = "model.test.my_model"
        my_model_columns = manifest.nodes[model_id].columns
        my_model_config = manifest.nodes[model_id].config
        contract_actual_config = my_model_config.contract

        assert contract_actual_config is True

        expected_columns = "{'id': ColumnInfo(name='id', description='hello', meta={}, data_type='integer', constraints=['not null', 'primary key'], constraints_check='(id > 0)', quote=True, tags=[], _extra={}), 'color': ColumnInfo(name='color', description='', meta={}, data_type='text', constraints=None, constraints_check=None, quote=None, tags=[], _extra={}), 'date_day': ColumnInfo(name='date_day', description='', meta={}, data_type='date', constraints=None, constraints_check=None, quote=None, tags=[], _extra={})}"

        assert expected_columns == str(my_model_columns)


class TestProjectConstraintsEnabledConfigs:
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "models": {
                "test": {
                    "+contract": True,
                    "subdirectory": {
                        "+contract": False,
                    },
                }
            }
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model.sql": my_model_sql,
        }

    def test__project_error(self, project):
        with pytest.raises(ParsingError) as err_info:
            run_dbt(["parse"], expect_pass=False)

        error_message_expected = "NOT within a model file(ex: .sql, .py) or `dbt_project.yml`."
        assert error_message_expected in str(err_info)


class TestModelConstraintsEnabledConfigs:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model.sql": my_model_contract_sql,
        }

    def test__model_error(self, project):
        with pytest.raises(ParsingError) as err_info:
            run_dbt(["parse"], expect_pass=False)

        error_message_expected = "NOT within a model file(ex: .sql, .py) or `dbt_project.yml`."
        assert error_message_expected in str(err_info)


class TestModelLevelConstraintsDisabledConfigs:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model.sql": my_model_constraints_disabled_sql,
            "constraints_schema.yml": model_schema_yml,
        }

    def test__model_contract_false(self, project):

        run_dbt(["parse"])
        manifest = get_manifest(project.project_root)
        model_id = "model.test.my_model"
        my_model_config = manifest.nodes[model_id].config
        contract_actual_config = my_model_config.contract

        assert contract_actual_config is False


class TestModelLevelConstraintsErrorMessages:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model.sql": my_view_model_sql,
            "constraints_schema.yml": model_schema_errors_yml,
        }

    def test__config_errors(self, project):
        with pytest.raises(ParsingError) as err_info:
            run_dbt(["parse"], expect_pass=False)

        expected_materialization_error = "Materialization Error: {'materialization': 'view'}"
        expected_empty_data_type_error = "Columns with `data_type` Blank/Null Errors: {'date_day'}"
        assert expected_materialization_error in str(err_info)
        assert expected_empty_data_type_error in str(err_info)


class TestSchemaConstraintsEnabledConfigs:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model.sql": my_model_sql,
            "constraints_schema.yml": model_schema_blank_yml,
        }

    def test__schema_error(self, project):
        with pytest.raises(ParsingError) as err_info:
            run_dbt(["parse"], expect_pass=False)

        schema_error_expected = "Schema Error: `yml` configuration does NOT exist"
        assert schema_error_expected in str(err_info)


class TestPythonModelLevelConstraintsErrorMessages:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "python_model.py": my_model_python_error,
            "constraints_schema.yml": model_schema_errors_yml,
        }

    def test__python_errors(self, project):
        with pytest.raises(ParsingError) as err_info:
            run_dbt(["parse"], expect_pass=False)

        expected_python_error = "Language Error: {'language': 'python'}"
        assert expected_python_error in str(err_info)
