import pytest
from dbt.tests.adapter.utils.base_utils import BaseUtils
from dbt.tests.adapter.utils.fixture_last_day import (
    seeds__data_last_day_csv,
    models__test_last_day_sql,
    models__test_last_day_yml,
)


class BaseLastDay(BaseUtils):
    @pytest.fixture(scope="class")
    def seeds(self):
        return {"data_last_day.csv": seeds__data_last_day_csv}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "test_last_day.yml": models__test_last_day_yml,
            "test_last_day.sql": self.interpolate_macro_namespace(
                models__test_last_day_sql, "last_day"
            ),
        }


class TestLastDay(BaseLastDay):
    pass
