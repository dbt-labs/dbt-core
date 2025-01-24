import pytest

from dbt.events.types import JinjaLogInfo
from dbt.tests.util import run_dbt
from tests.utils import EventCatcher

sample_mode_model_sql = """
{{ config(materialized='table') }}

{% if execute %}
    {{ log("Sample mode: " ~ invocation_args_dict.get("sample"), info=true) }}
{% endif %}

select 1 as id, TIMESTAMP '2020-01-01 00:00:00-0' as event_time
"""


class TestSampleModeInJinjaContext:

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "sample_mode_model.sql": sample_mode_model_sql,
        }

    @pytest.fixture
    def event_catcher(self) -> EventCatcher:
        return EventCatcher(event_to_catch=JinjaLogInfo)  # type: ignore

    @pytest.mark.parametrize(
        "use_sample_mode,arg_value_in_jinja",
        [
            (True, True),
            (False, False),
        ],
    )
    def test_sample_mode(
        self, project, event_catcher: EventCatcher, use_sample_mode: bool, arg_value_in_jinja: bool
    ):
        run_args = ["run"]
        if use_sample_mode:
            run_args.append("--sample")

        _ = run_dbt(run_args, callbacks=[event_catcher.catch])
        assert len(event_catcher.caught_events) == 1
        assert event_catcher.caught_events[0].info.msg == f"Sample mode: {arg_value_in_jinja}"  # type: ignore
