import pytest
from dbt.tests.util import run_dbt
from dbt.exceptions import CompilationException


descendant_sql = """
-- should be ref('model')
select * from {{ ref(model) }}
"""


model_sql = """
select 1 as id
"""


@pytest.fixture(scope="class")
def models():
    return {
        "descendant.sql": descendant_sql,
        "model.sql": model_sql,
    }


def test_undefined_value(project):
    # Tests that a project with an invalid reference fails
    with pytest.raises(CompilationException):
        run_dbt(["compile"])
