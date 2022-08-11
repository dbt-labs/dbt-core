import pytest
from dbt.tests.adapter.utils.base_utils import BaseUtils
from dbt.tests.adapter.utils.fixture_replace import (
    seeds__data_replace_csv,
    models__test_replace_sql,
    models__test_replace_yml,
)


class BaseReplace(BaseUtils):
    @pytest.fixture(scope="class")
    def seeds(self):
        return {"data_replace.csv": seeds__data_replace_csv}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "test_replace.yml": models__test_replace_yml,
            "test_replace.sql": self.interpolate_macro_namespace(
                models__test_replace_sql, "replace"
            ),
        }


class TestReplace(BaseReplace):
    pass
