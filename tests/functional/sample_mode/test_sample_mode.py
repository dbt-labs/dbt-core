from datetime import datetime

import freezegun
import pytest
import pytz

from dbt.artifacts.resources.types import BatchSize
from dbt.event_time.sample_window import SampleWindow
from dbt.events.types import JinjaLogInfo
from dbt.materializations.incremental.microbatch import MicrobatchBuilder
from dbt.tests.util import read_file, relation_from_name, run_dbt
from tests.utils import EventCatcher

input_model_sql = """
{{ config(materialized='table', event_time='event_time') }}
select 1 as id, TIMESTAMP '2020-01-01 01:25:00-0' as event_time
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

sample_microbatch_model_sql = """
{{ config(materialized='incremental', incremental_strategy='microbatch', event_time='event_time', batch_size='day', lookback=3, begin='2024-12-25', unique_key='id')}}

{% if execute %}
    {{ log("batch.event_time_start: "~ model.batch.event_time_start, info=True)}}
    {{ log("batch.event_time_end: "~ model.batch.event_time_end, info=True)}}
{% endif %}

SELECT * FROM {{ ref("input_model") }}
"""


class BaseSampleMode:
    # TODO This is now used in 3 test files, it might be worth turning into a full test utility method
    def assert_row_count(self, project, relation_name: str, expected_row_count: int):
        relation = relation_from_name(project.adapter, relation_name)
        result = project.run_sql(f"select count(*) as num_rows from {relation}", fetch="one")

        if result[0] != expected_row_count:
            # running show for debugging
            run_dbt(["show", "--inline", f"select * from {relation}"])

            assert result[0] == expected_row_count


class TestBasicSampleMode(BaseSampleMode):
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


class TestMicrobatchSampleMode(BaseSampleMode):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "input_model.sql": input_model_sql,
            "sample_microbatch_model.sql": sample_microbatch_model_sql,
        }

    @pytest.fixture
    def event_time_start_catcher(self) -> EventCatcher:
        return EventCatcher(event_to_catch=JinjaLogInfo, predicate=lambda event: "batch.event_time_start" in event.info.msg)  # type: ignore

    @pytest.fixture
    def event_time_end_catcher(self) -> EventCatcher:
        return EventCatcher(event_to_catch=JinjaLogInfo, predicate=lambda event: "batch.event_time_end" in event.info.msg)  # type: ignore

    @freezegun.freeze_time("2025-01-03T02:03:0Z")
    def test_sample_mode(
        self,
        project,
        event_time_end_catcher: EventCatcher,
        event_time_start_catcher: EventCatcher,
    ):
        expected_batches = [
            ("2025-01-01 00:00:00", "2025-01-02 00:00:00"),
            ("2025-01-02 00:00:00", "2025-01-03 00:00:00"),
            ("2025-01-03 00:00:00", "2025-01-04 00:00:00"),
        ]

        # These are different from the expected batches because the sample window might only operate on "part" of a given batch
        expected_filters = [
            (
                "event_time >= '2025-01-01 02:03:00+00:00' and event_time < '2025-01-02 00:00:00+00:00'"
            ),
            (
                "event_time >= '2025-01-02 00:00:00+00:00' and event_time < '2025-01-03 00:00:00+00:00'"
            ),
            (
                "event_time >= '2025-01-03 00:00:00+00:00' and event_time < '2025-01-03 02:03:00+00:00'"
            ),
        ]

        _ = run_dbt(
            ["run", "--sample", "--sample-window=2 day"],
            callbacks=[event_time_end_catcher.catch, event_time_start_catcher.catch],
        )
        assert len(event_time_start_catcher.caught_events) == len(expected_batches)
        assert len(event_time_end_catcher.caught_events) == len(expected_batches)

        for index in range(len(expected_batches)):
            assert expected_batches[index][0] in event_time_start_catcher.caught_events[index].info.msg  # type: ignore
            assert expected_batches[index][1] in event_time_end_catcher.caught_events[index].info.msg  # type: ignore

            batch_id = MicrobatchBuilder.format_batch_start(
                datetime.fromisoformat(expected_batches[index][0]), BatchSize.day
            )
            batch_file_name = f"sample_microbatch_model_{batch_id}.sql"
            compiled_sql = read_file(
                project.project_root,
                "target",
                "compiled",
                "test",
                "models",
                "sample_microbatch_model",
                batch_file_name,
            )
            assert expected_filters[index] in compiled_sql

        # The first row of the "input_model" should be excluded from the sample because
        # it falls outside of the filter for the first batch (which is only doing a _partial_ batch selection)
        self.assert_row_count(
            project=project,
            relation_name="sample_microbatch_model",
            expected_row_count=2,
        )
