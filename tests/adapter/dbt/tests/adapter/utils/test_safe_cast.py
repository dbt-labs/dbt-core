import pytest
from dbt.tests.adapter.utils.base_utils import BaseUtils
from dbt.tests.adapter.utils.fixture_safe_cast import (
    seeds__data_safe_cast_csv,
    models__test_safe_cast_sql,
    models__test_safe_cast_yml,
)


class BaseSafeCast(BaseUtils):
    @pytest.fixture(scope="class")
    def seeds(self):
        return {"data_safe_cast.csv": seeds__data_safe_cast_csv}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "test_safe_cast.yml": models__test_safe_cast_yml,
            "test_safe_cast.sql": self.interpolate_macro_namespace(
                self.interpolate_macro_namespace(models__test_safe_cast_sql, "safe_cast"),
                "type_string",
            ),
        }


class TestSafeCast(BaseSafeCast):
    pass
