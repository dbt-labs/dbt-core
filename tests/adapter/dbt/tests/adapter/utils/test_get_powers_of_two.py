import pytest
from dbt.tests.adapter.utils.base_utils import BaseUtils
from dbt.tests.adapter.utils.fixture_get_powers_of_two import (
    models__test_get_powers_of_two_sql,
    models__test_get_powers_of_two_yml,
)


class BaseGetPowersOfTwo(BaseUtils):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "test_get_powers_of_two.yml": models__test_get_powers_of_two_yml,
            "test_get_powers_of_two.sql": self.interpolate_macro_namespace(
                models__test_get_powers_of_two_sql, "get_powers_of_two"
            ),
        }


class TestGetPowersOfTwo(BaseGetPowersOfTwo):
    pass
