import pytest
from dbt.tests.adapter.utils.base_utils import BaseUtils
from dbt.tests.adapter.utils.fixture_string_literal import (
    models__test_string_literal_sql,
    models__test_string_literal_yml,
)


class BaseStringLiteral(BaseUtils):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "test_string_literal.yml": models__test_string_literal_yml,
            "test_string_literal.sql": self.interpolate_macro_namespace(
                models__test_string_literal_sql, "string_literal"
            ),
        }


class TestStringLiteral(BaseStringLiteral):
    pass
