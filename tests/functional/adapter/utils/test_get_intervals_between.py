import pytest
from tests.functional.adapter.utils.base_utils import BaseUtils
from tests.functional.adapter.utils.fixture_get_intervals_between import (
    models__test_get_intervals_between_sql,
    models__test_get_intervals_between_yml,
)


class BaseGetIntervalsBetween(BaseUtils):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "test_get_intervals_between.yml": models__test_get_intervals_between_yml,
            "test_get_intervals_between.sql": self.interpolate_macro_namespace(
                models__test_get_intervals_between_sql, "get_intervals_between"
            ),
        }


class TestGetIntervalsBetween(BaseGetIntervalsBetween):
    pass
