import pytest
from dbt.tests.adapter.utils.base_utils import BaseUtils
from dbt.tests.adapter.utils.fixture_hash import (
    seeds__data_hash_csv,
    models__test_hash_sql,
    models__test_hash_yml,
)


class BaseHash(BaseUtils):
    @pytest.fixture(scope="class")
    def seeds(self):
        return {"data_hash.csv": seeds__data_hash_csv}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "test_hash.yml": models__test_hash_yml,
            "test_hash.sql": self.interpolate_macro_namespace(models__test_hash_sql, "hash"),
        }


class TestHash(BaseHash):
    pass
