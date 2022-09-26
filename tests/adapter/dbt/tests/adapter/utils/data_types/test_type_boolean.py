import pytest
from dbt.tests.adapter.utils.data_types.base_data_type_macro import BaseDataTypeMacro

seeds__expected_csv = """boolean_col
True
""".lstrip()

models__actual_sql = """
select cast('True' as {{ type_boolean() }}) as boolean_col
"""


class BaseTypeBoolean(BaseDataTypeMacro):
    @pytest.fixture(scope="class")
    def seeds(self):
        return {"expected.csv": seeds__expected_csv}

    @pytest.fixture(scope="class")
    def models(self):
        return {"actual.sql": self.interpolate_macro_namespace(models__actual_sql, "type_boolean")}


class TestTypeBoolean(BaseTypeBoolean):
    pass
