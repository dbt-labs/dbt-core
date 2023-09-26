import pytest
from dbt.tests.adapter.utils.base_utils import BaseUtils
from dbt.tests.adapter.utils.fixture_generate_series import (
    models__test_generate_series_sql,
    models__test_generate_series_yml,
)


class BaseGenerateSeries(BaseUtils):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "test_generate_series.yml": models__test_generate_series_yml,
            "test_generate_series.sql": self.interpolate_macro_namespace(
                models__test_generate_series_sql, "generate_series"
            ),
        }


class TestGenerateSeries(BaseGenerateSeries):
    pass
