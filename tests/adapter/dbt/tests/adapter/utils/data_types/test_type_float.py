import pytest
from dbt.tests.adapter.utils.data_types.base_data_type_macro import BaseDataTypeMacro

seeds__expected_csv = """float_col
1.2345
""".lstrip()

models__actual_sql = """
select cast('1.2345' as {{ type_float() }}) as float_col
"""


class BaseTypeFloat(BaseDataTypeMacro):
    @pytest.fixture(scope="class")
    def seeds(self):
        return {"expected.csv": seeds__expected_csv}

    @pytest.fixture(scope="class")
    def models(self):
        return {"actual.sql": self.interpolate_macro_namespace(models__actual_sql, "type_float")}


class TestTypeFloat(BaseTypeFloat):
    pass
