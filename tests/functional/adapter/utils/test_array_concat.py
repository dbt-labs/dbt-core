import pytest
from tests.functional.adapter.utils.base_array_utils import BaseArrayUtils
from tests.functional.adapter.utils.fixture_array_concat import (
    models__array_concat_actual_sql,
    models__array_concat_expected_sql,
)


class BaseArrayConcat(BaseArrayUtils):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "actual.sql": models__array_concat_actual_sql,
            "expected.sql": models__array_concat_expected_sql,
        }


class TestArrayConcat(BaseArrayConcat):
    pass
