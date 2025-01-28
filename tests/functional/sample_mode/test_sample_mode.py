from datetime import datetime

import freezegun
import pytest
import pytz

from dbt.event_time.sample_window import SampleWindow
from dbt.events.types import JinjaLogInfo
from dbt.tests.util import relation_from_name, run_dbt
from tests.utils import EventCatcher

input_model_sql = """
{{ config(materialized='table', event_time='event_time') }}
select 1 as id, TIMESTAMP '2020-01-01 11:25:00-0' as event_time
UNION ALL
select 2 as id, TIMESTAMP '2025-01-01 13:47:00-0' as event_time
UNION ALL
select 3 as id, TIMESTAMP '2025-01-02 12:32:00-0' as event_time
"""

sample_mode_model_sql = """
{{ config(materialized='table', event_time='event_time') }}

{% if execute %}
    {{ log("Sample mode: " ~ invocation_args_dict.get("sample"), info=true) }}
    {{ log("Sample window: " ~ invocation_args_dict.get("sample_window"), info=true) }}
{% endif %}

SELECT * FROM {{ ref("input_model") }}
"""


class TestSampleMode:

    def assert_row_count(self, project, relation_name: str, expected_row_count: int):
        relation = relation_from_name(project.adapter, relation_name)
        result = project.run_sql(f"select count(*) as num_rows from {relation}", fetch="one")

        if result[0] != expected_row_count:
            # running show for debugging
            run_dbt(["show", "--inline", f"select * from {relation}"])

            assert result[0] == expected_row_count

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "input_model.sql": input_model_sql,
            "sample_mode_model.sql": sample_mode_model_sql,
        }

    @pytest.fixture
    def event_catcher(self) -> EventCatcher:
        return EventCatcher(event_to_catch=JinjaLogInfo)  # type: ignore

    @pytest.mark.parametrize(
        "use_sample_mode,expected_row_count,arg_value_in_jinja",
        [
            (True, 1, True),
            (False, 3, False),
        ],
    )
    @freezegun.freeze_time("2025-01-03T02:03:0Z")
    def test_sample_mode(
        self,
        project,
        event_catcher: EventCatcher,
        use_sample_mode: bool,
        expected_row_count: int,
        arg_value_in_jinja: bool,
    ):
        run_args = ["run"]
        expected_sample_window = None
        if use_sample_mode:
            run_args.extend(["--sample", "--sample-window=1 day"])
            expected_sample_window = SampleWindow(
                start=datetime(2025, 1, 2, 2, 3, 0, 0, tzinfo=pytz.UTC),
                end=datetime(2025, 1, 3, 2, 3, 0, 0, tzinfo=pytz.UTC),
            )

        _ = run_dbt(run_args, callbacks=[event_catcher.catch])
        assert len(event_catcher.caught_events) == 2
        assert event_catcher.caught_events[0].info.msg == f"Sample mode: {arg_value_in_jinja}"  # type: ignore
        assert event_catcher.caught_events[1].info.msg == f"Sample window: {expected_sample_window}"  # type: ignore
        self.assert_row_count(
            project=project,
            relation_name="sample_mode_model",
            expected_row_count=expected_row_count,
        )
