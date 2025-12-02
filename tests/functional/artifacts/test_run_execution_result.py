import pytest
from dateutil.tz import tzutc

from dbt.contracts.results import RunExecutionResult
from dbt.tests.util import run_dbt, write_file

sample_model_sql = """
select 1 as id
"""


@pytest.fixture(scope="function", autouse=True)
def sample_model(project):
    write_file(
        sample_model_sql,
        project.project_root,
        "models",
        "model.sql",
    )


def test_run_execution_result_compiled_serialization(project):

    result = run_dbt(["compile"])
    result_from_dict = RunExecutionResult.from_dict(result.to_dict())

    assert isinstance(result, RunExecutionResult)
    assert len(result.results) > 0
    assert result.results[0].status.name == "Success"

    assert result_from_dict.args == result.args
    assert len(result_from_dict.results) == len(result.results)

    assert result.generated_at.tzinfo is None
    assert result_from_dict.generated_at.tzinfo == tzutc()
    assert result_from_dict.generated_at.replace(tzinfo=None) == result.generated_at

    for original in result.results:
        for deserialized in result_from_dict.results:
            if original.node.unique_id == deserialized.node.unique_id:
                assert original.node.created_at == deserialized.node.created_at
