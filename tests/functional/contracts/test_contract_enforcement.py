import pytest

from dbt.tests.util import run_dbt, write_file

my_model_sql = """
select 'some string' as string_column
"""

my_model_int_sql = """
select 123 as int_column
"""

model_schema_yml = """
models:
  - name: my_model
    config:
      materialized: incremental
      on_schema_change: append_new_columns
      contract: {enforced: true}
    columns:
      - name: string_column
        data_type: text
"""


class TestIncrementalModelContractEnforcement:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model.sql": my_model_sql,
            "schema.yml": model_schema_yml,
        }

    def test_contracted_incremental(self, project):
        results = run_dbt()
        assert len(results) == 1
        # now update the column type in the model to break the contract
        write_file(my_model_int_sql, project.project_root, "models", "my_model.sql")

        expected_msg = "This model has an enforced contract that failed."
        results = run_dbt(expect_pass=False)
        assert len(results) == 1
        msg = results[0].message
        assert expected_msg in msg
