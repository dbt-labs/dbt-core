import pytest
from tests.functional.adapter.utils.base_array_utils import BaseArrayUtils
from tests.functional.adapter.utils.fixture_array_append import (
    models__array_append_actual_sql,
    models__array_append_expected_sql,
)


class BaseArrayAppend(BaseArrayUtils):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "actual.sql": models__array_append_actual_sql,
            "expected.sql": models__array_append_expected_sql,
        }


class TestArrayAppend(BaseArrayAppend):
    pass
