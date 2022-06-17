import pytest
from dbt.tests.adapter.utils.base_utils import BaseUtils
from dbt.tests.adapter.utils.fixture_length import (
    seeds__data_length_csv,
    models__test_length_sql,
    models__test_length_yml,
)


class BaseLength(BaseUtils):
    @pytest.fixture(scope="class")
    def seeds(self):
        return {"data_length.csv": seeds__data_length_csv}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "test_length.yml": models__test_length_yml,
            "test_length.sql": self.interpolate_macro_namespace(models__test_length_sql, "length"),
        }


class TestLength(BaseLength):
    pass
