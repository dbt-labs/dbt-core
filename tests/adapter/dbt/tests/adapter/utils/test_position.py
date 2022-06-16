import pytest
from dbt.tests.adapter.utils.base_utils import BaseUtils
from dbt.tests.adapter.utils.fixture_position import (
    seeds__data_position_csv,
    models__test_position_sql,
    models__test_position_yml,
)


class BasePosition(BaseUtils):
    @pytest.fixture(scope="class")
    def seeds(self):
        return {"data_position.csv": seeds__data_position_csv}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "test_position.yml": models__test_position_yml,
            "test_position.sql": self.interpolate_macro_namespace(
                models__test_position_sql, "position"
            ),
        }


class TestPosition(BasePosition):
    pass
