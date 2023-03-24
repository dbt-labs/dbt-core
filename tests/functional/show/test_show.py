import pytest

from dbt.exceptions import DbtRuntimeError
from dbt.tests.util import run_dbt_and_capture
from tests.functional.compile.fixtures import (
    first_model_sql,
    second_model_sql,
    schema_yml,
)


class TestShow:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "first_model.sql": first_model_sql,
            "second_model.sql": second_model_sql,
            "schema.yml": schema_yml,
        }

    def test_none(self, project):
        with pytest.raises(
            DbtRuntimeError, match="Either --select or --inline must be passed to show"
        ):
            run_dbt_and_capture(["show"])

    def test_multiple_select(self, project):
        with pytest.raises(DbtRuntimeError, match="Database Error in model second_model"):
            run_dbt_and_capture(["show", "--select", "second_model"])
