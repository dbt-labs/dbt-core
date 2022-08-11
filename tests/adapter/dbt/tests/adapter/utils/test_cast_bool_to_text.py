import pytest
from dbt.tests.adapter.utils.base_utils import BaseUtils
from dbt.tests.adapter.utils.fixture_cast_bool_to_text import (
    models__test_cast_bool_to_text_sql,
    models__test_cast_bool_to_text_yml,
)


class BaseCastBoolToText(BaseUtils):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "test_cast_bool_to_text.yml": models__test_cast_bool_to_text_yml,
            "test_cast_bool_to_text.sql": self.interpolate_macro_namespace(
                models__test_cast_bool_to_text_sql, "cast_bool_to_text"
            ),
        }


class TestCastBoolToText(BaseCastBoolToText):
    pass
