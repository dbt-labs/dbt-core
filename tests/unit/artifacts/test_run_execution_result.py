from dateutil.tz import tzutc
from hypothesis import given
from hypothesis.strategies import (
    builds,
    composite,
    dictionaries,
    floats,
    lists,
    sampled_from,
    text,
)

from dbt.contracts.results import RunExecutionResult, RunResult, RunStatus, TimingInfo
from tests.unit.fixtures import model_node


@composite
def run_result_strategy(draw):
    node = model_node()
    status = draw(sampled_from(list(RunStatus)))
    message = draw(text(min_size=0, max_size=30) | sampled_from([None]))
    result = RunResult.from_node(node=node, status=status, message=message)
    result.execution_time = draw(floats(min_value=0.0, max_value=30.0))
    result.timing = draw(lists(builds(TimingInfo), max_size=2))

    return result


@given(
    args=dictionaries(text(min_size=1, max_size=10), text(min_size=1, max_size=10)),
    elapsed_time=floats(min_value=0.0, max_value=30.0),
    results=lists(run_result_strategy(), min_size=1, max_size=3),
)
def test_run_execution_result_serialization(args, elapsed_time, results):

    obj = RunExecutionResult(results=results, elapsed_time=elapsed_time, args=args)
    obj_from_dict = RunExecutionResult.from_dict(obj.to_dict())

    assert obj_from_dict.args == obj.args
    assert len(obj_from_dict.results) == len(obj.results)

    assert obj.generated_at.tzinfo is None
    assert obj_from_dict.generated_at.tzinfo == tzutc()
    assert obj_from_dict.generated_at.replace(tzinfo=None) == obj.generated_at

    for original, deserialized in zip(obj.results, obj_from_dict.results):
        assert original.node.created_at == deserialized.node.created_at
