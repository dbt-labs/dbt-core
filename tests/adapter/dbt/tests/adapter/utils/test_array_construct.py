import pytest
from dbt.tests.adapter.utils.base_array_utils import BaseArrayUtils
from dbt.tests.adapter.utils.fixture_array_construct import (
    models__array_construct_actual_sql,
    models__array_construct_expected_sql,
)


class BaseArrayConstruct(BaseArrayUtils):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "actual.sql": models__array_construct_actual_sql,
            "expected.sql": models__array_construct_expected_sql,
        }


class TestArrayConstruct(BaseArrayConstruct):
    pass
