import pytest
from dbt.tests.adapter.utils.data_types.base_data_type_macro import BaseDataTypeMacro

seeds__expected_csv = """numeric_col
1.2345
""".lstrip()

# need to explicitly cast this to avoid it being a double/float
seeds__expected_yml = """
version: 2
seeds:
  - name: expected
    config:
      column_types:
        numeric_col: {}
"""

models__actual_sql = """
select cast('1.2345' as {{ type_numeric() }}) as numeric_col
"""


class BaseTypeNumeric(BaseDataTypeMacro):
    def numeric_fixture_type(self):
        return "numeric(28,6)"

    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "expected.csv": seeds__expected_csv,
            "expected.yml": seeds__expected_yml.format(self.numeric_fixture_type()),
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {"actual.sql": self.interpolate_macro_namespace(models__actual_sql, "type_numeric")}


class TestTypeNumeric(BaseTypeNumeric):
    pass
