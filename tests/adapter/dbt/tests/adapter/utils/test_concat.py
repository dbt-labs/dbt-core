import pytest
from dbt.tests.adapter.utils.base_utils import BaseUtils
from dbt.tests.adapter.utils.fixture_concat import (
    seeds__data_concat_csv,
    models__test_concat_sql,
    models__test_concat_yml,
)


class BaseConcat(BaseUtils):
    @pytest.fixture(scope="class")
    def seeds(self):
        return {"data_concat.csv": seeds__data_concat_csv}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "test_concat.yml": models__test_concat_yml,
            "test_concat.sql": self.interpolate_macro_namespace(models__test_concat_sql, "concat"),
        }


class TestConcat(BaseConcat):
    pass
