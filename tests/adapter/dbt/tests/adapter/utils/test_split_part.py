import pytest
from dbt.tests.adapter.utils.base_utils import BaseUtils
from dbt.tests.adapter.utils.fixture_split_part import (
    seeds__data_split_part_csv,
    models__test_split_part_sql,
    models__test_split_part_yml,
)


class BaseSplitPart(BaseUtils):
    @pytest.fixture(scope="class")
    def seeds(self):
        return {"data_split_part.csv": seeds__data_split_part_csv}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "test_split_part.yml": models__test_split_part_yml,
            "test_split_part.sql": self.interpolate_macro_namespace(
                models__test_split_part_sql, "split_part"
            ),
        }


class TestSplitPart(BaseSplitPart):
    pass
