import pytest
from dbt.tests.adapter.utils.data_types.base_data_type_macro import BaseDataTypeMacro

seeds__expected_csv = """timestamp_col
2021-01-01 01:01:01
""".lstrip()

# need to explicitly cast this to avoid it being a DATETIME on BigQuery
# (but - should it actually be a DATETIME, for consistency with other dbs?)
seeds__expected_yml = """
version: 2
seeds:
  - name: expected
    config:
      column_types:
        timestamp_col: timestamp
"""

models__actual_sql = """
select cast('2021-01-01 01:01:01' as {{ type_timestamp() }}) as timestamp_col
"""


class BaseTypeTimestamp(BaseDataTypeMacro):
    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "expected.csv": seeds__expected_csv,
            "expected.yml": seeds__expected_yml,
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "actual.sql": self.interpolate_macro_namespace(models__actual_sql, "type_timestamp")
        }


class TestTypeTimestamp(BaseTypeTimestamp):
    pass
