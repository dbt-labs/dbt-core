import pytest
from dbt.tests.adapter.utils.data_types.base_data_type_macro import BaseDataTypeMacro

seeds__expected_csv = """int_col
12345678
""".lstrip()

models__actual_sql = """
select cast('12345678' as {{ type_int() }}) as int_col
"""


class BaseTypeInt(BaseDataTypeMacro):
    @pytest.fixture(scope="class")
    def seeds(self):
        return {"expected.csv": seeds__expected_csv}

    @pytest.fixture(scope="class")
    def models(self):
        return {"actual.sql": self.interpolate_macro_namespace(models__actual_sql, "type_int")}


class TestTypeInt(BaseTypeInt):
    pass
