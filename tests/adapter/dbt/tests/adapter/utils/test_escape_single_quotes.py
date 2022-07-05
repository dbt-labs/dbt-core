import pytest
from dbt.tests.adapter.utils.base_utils import BaseUtils
from dbt.tests.adapter.utils.fixture_escape_single_quotes import (
    models__test_escape_single_quotes_quote_sql,
    models__test_escape_single_quotes_backslash_sql,
    models__test_escape_single_quotes_yml,
)


class BaseEscapeSingleQuotesQuote(BaseUtils):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "test_escape_single_quotes.yml": models__test_escape_single_quotes_yml,
            "test_escape_single_quotes.sql": self.interpolate_macro_namespace(
                models__test_escape_single_quotes_quote_sql, "escape_single_quotes"
            ),
        }


class BaseEscapeSingleQuotesBackslash(BaseUtils):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "test_escape_single_quotes.yml": models__test_escape_single_quotes_yml,
            "test_escape_single_quotes.sql": self.interpolate_macro_namespace(
                models__test_escape_single_quotes_backslash_sql, "escape_single_quotes"
            ),
        }


class TestEscapeSingleQuotes(BaseEscapeSingleQuotesQuote):
    pass
