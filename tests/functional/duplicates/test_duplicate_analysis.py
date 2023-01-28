import pytest

from dbt.exceptions import CompilationError
from dbt.tests.util import run_dbt


my_model_sql = """
select 1 as id
"""

my_analysis_sql = """
select * from {{ ref('my_model') }}
"""


class TestDuplicateAnalysis:
    @pytest.fixture(scope="class")
    def models(self):
        return {"my_model.sql": my_model_sql}

    @pytest.fixture(scope="class")
    def analyses(self):
        return {
            "anlysis-1": {"model.sql": my_analysis_sql},
            "anlysis-2": {"model.sql": my_analysis_sql},
        }

    def test_duplicate_model_enabled(self, project):
        message = "dbt found two analyses with the name"
        with pytest.raises(CompilationError) as exc:
            run_dbt(["compile"])
        exc_str = " ".join(str(exc.value).split())  # flatten all whitespace
        assert message in exc_str
