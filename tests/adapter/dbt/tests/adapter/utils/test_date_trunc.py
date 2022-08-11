import pytest
from dbt.tests.adapter.utils.base_utils import BaseUtils
from dbt.tests.adapter.utils.fixture_date_trunc import (
    seeds__data_date_trunc_csv,
    models__test_date_trunc_sql,
    models__test_date_trunc_yml,
)


class BaseDateTrunc(BaseUtils):
    @pytest.fixture(scope="class")
    def seeds(self):
        return {"data_date_trunc.csv": seeds__data_date_trunc_csv}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "test_date_trunc.yml": models__test_date_trunc_yml,
            "test_date_trunc.sql": self.interpolate_macro_namespace(
                models__test_date_trunc_sql, "date_trunc"
            ),
        }


class TestDateTrunc(BaseDateTrunc):
    pass
