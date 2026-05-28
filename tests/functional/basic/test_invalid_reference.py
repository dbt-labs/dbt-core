import pytest

from dbt.exceptions import CompilationError
from tests.functional.v2_parser_parity.v2_self_parser import (
    run_dbt_for_mode,
    xfail_v2_self,
)

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


@pytest.mark.v2_parser_parity
def test_undefined_value(project, parser_mode):
    # Tests that a project with an invalid reference fails
    xfail_v2_self(
        parser_mode,
        "see v2_parser_parity/README.md: parse-time CompilationError is wrapped/replaced under v2 dispatch",
    )
    with pytest.raises(CompilationError):
        run_dbt_for_mode(parser_mode, ["compile"])
