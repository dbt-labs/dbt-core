import pytest

from dbt.tests.adapter.utils.base_utils import BaseUtils
from dbt.tests.adapter.utils.fixture_null_compare import (
    MODELS__TEST_MIXED_NULL_COMPARE_SQL,
    MODELS__TEST_MIXED_NULL_COMPARE_YML,
    MODELS__TEST_NULL_COMPARE_SQL,
    MODELS__TEST_NULL_COMPARE_YML,
)
from dbt.tests.util import run_dbt


class BaseMixedNullCompare(BaseUtils):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "test_mixed_null_compare.yml": MODELS__TEST_MIXED_NULL_COMPARE_YML,
            "test_mixed_null_compare.sql": MODELS__TEST_MIXED_NULL_COMPARE_SQL,
        }

    def test_build_assert_equal(self, project):
        run_dbt()
        run_dbt(["test"], expect_pass=False)


class BaseNullCompare(BaseUtils):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "test_null_compare.yml": MODELS__TEST_NULL_COMPARE_YML,
            "test_null_compare.sql": MODELS__TEST_NULL_COMPARE_SQL,
        }


class TestMixedNullCompare(BaseMixedNullCompare):
    pass


class TestNullCompare(BaseNullCompare):
    pass
