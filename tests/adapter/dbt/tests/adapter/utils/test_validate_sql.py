from typing import Type

import pytest

from dbt.adapters.base.impl import BaseAdapter
from dbt.exceptions import DbtRuntimeError, InvalidConnectionError


class BaseValidateSqlMethod:
    """Tests the behavior of the validate_sql method for the relevant adapters.

    The valid and invalid SQL should work with most engines by default, but
    both inputs can be overridden as needed for a given engine to get the correct
    behavior.

    The base method is meant to throw the appropriate custom exception when validate_sql
    fails.
    """

    @pytest.fixture(scope="class")
    def valid_sql(self) -> str:
        """Returns a valid statement for issuing as a validate_sql query.

        Ideally this would be checkable for non-execution. For example, we could use a
        CREATE TABLE statement with an assertion that no table was created. However,
        for most adapter types this is unnecessary - the EXPLAIN keyword has exactly the
        behavior we want, and here we are essentially testing to make sure it is
        supported. As such, we return a simple SELECT query, and leave it to
        engine-specific test overrides to specify more detailed behavior as appropriate.
        """

        return "select 1"

    @pytest.fixture(scope="class")
    def invalid_sql(self) -> str:
        """Returns an invalid statement for issuing a bad validate_sql query."""

        return "Let's run some invalid SQL and see if we get an error!"

    @pytest.fixture(scope="class")
    def expected_exception(self) -> Type[Exception]:
        """Returns the Exception type thrown by a failed query.

        Defaults to dbt.exceptions.DbtRuntimeError because that is the most common
        base exception for adapters to throw."""
        return DbtRuntimeError

    def test_validate_sql_success(self, adapter: BaseAdapter, valid_sql: str) -> None:
        """Executes validate_sql on valid SQL. No news is good news."""
        with adapter.connection_named("test_valid_sql_validation"):
            adapter.validate_sql(valid_sql)

    def test_validate_sql_failure(
        self,
        adapter: BaseAdapter,
        invalid_sql: str,
        expected_exception: Type[Exception],
    ) -> None:
        """Executes validate_sql on invalid SQL, expecting the exception."""
        with pytest.raises(expected_exception=expected_exception) as excinfo:
            with adapter.connection_named("test_invalid_sql_validation"):
                adapter.validate_sql(invalid_sql)

        # InvalidConnectionError is a subclass of DbtRuntimeError, so we have to handle
        # it separately.
        if excinfo.type == InvalidConnectionError:
            raise ValueError(
                "Unexpected InvalidConnectionError. This typically indicates a problem "
                "with the test setup, rather than the expected error for an invalid "
                "validate_sql query."
            ) from excinfo.value


class TestValidateSqlMethod(BaseValidateSqlMethod):
    pass
