import pytest
from dbt.tests.util import run_dbt
from dbt.exceptions import CompilationException

python_model = """
def model( dbt, session):
    dbt.config(
        materialized='view'
    )
    df = df.limit(2)
    return df
"""


class TestManifestLoad:
    @pytest.fixture(scope="class")
    def models(self):
        return {"python_model.py": python_model}

    def test_invalid_materalization(self, project):
        try:
            run_dbt(["compile"], expect_pass=False)
        except CompilationException as e:
            assert (
                e.msg
                == "'view' is not a supported materialization of python model for the current adapter,\ncurrent adapter support ['table']"
            )
