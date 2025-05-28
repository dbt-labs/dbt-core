from datetime import datetime
from typing import Optional

import freezegun
import pytest
import pytz

from dbt.artifacts.resources.types import BatchSize
from dbt.event_time.sample_window import SampleWindow
from dbt.events.types import JinjaLogInfo
from dbt.materializations.incremental.microbatch import MicrobatchBuilder
from dbt.tests.util import read_file, relation_from_name, run_dbt, write_file
from tests.utils import EventCatcher

input_model_sql = """
{{ config(materialized='table', event_time='event_time') }}
select 1 as id, TIMESTAMP '2020-01-01 01:25:00-0' as event_time
UNION ALL
select 2 as id, TIMESTAMP '2025-01-02 13:47:00-0' as event_time
UNION ALL
select 3 as id, TIMESTAMP '2025-01-03 01:32:00-0' as event_time
"""

later_input_model_sql = """
{{ config(materialized='table', event_time='event_time') }}
select 1 as id, TIMESTAMP '2020-01-01 01:25:00-0' as event_time
UNION ALL
select 2 as id, TIMESTAMP '2025-01-02 13:47:00-0' as event_time
UNION ALL
select 3 as id, TIMESTAMP '2025-01-03 01:32:00-0' as event_time
UNION ALL
select 4 as id, TIMESTAMP '2025-01-04 14:32:00-0' as event_time
UNION ALL
select 5 as id, TIMESTAMP '2025-01-05 20:32:00-0' as event_time
UNION ALL
select 6 as id, TIMESTAMP '2025-01-06 12:32:00-0' as event_time
"""

input_seed_csv = """id,event_time
1,'2020-01-01 01:25:00-0'
2,'2025-01-02 13:47:00-0'
3,'2025-01-03 01:32:00-0'
"""

seed_properties_yml = """
seeds:
    - name: input_seed
      config:
        event_time: event_time
        column_types:
            event_time: timestamp
"""

sample_mode_model_sql = """
{{ config(materialized='table', event_time='event_time') }}

{% if execute %}
    {{ log("Sample: " ~ invocation_args_dict.get("sample"), info=true) }}
{% endif %}

SELECT * FROM {{ ref("input_model") }}
"""

sample_input_seed_sql = """
{{ config(materialized='table') }}

SELECT * FROM {{ ref("input_seed") }}
"""

sample_microbatch_model_sql = """
{{ config(materialized='incremental', incremental_strategy='microbatch', event_time='event_time', batch_size='day', lookback=3, begin='2024-12-25', unique_key='id')}}

{% if execute %}
    {{ log("batch.event_time_start: "~ model.batch.event_time_start, info=True)}}
    {{ log("batch.event_time_end: "~ model.batch.event_time_end, info=True)}}
{% endif %}

SELECT * FROM {{ ref("input_model") }}
"""

sample_incremental_merge_sql = """
{{ config(materialized='incremental', incremental_strategy='merge', unique_key='id')}}

{% if execute %}
    {{ log("is_incremental: " ~ is_incremental(), info=true) }}
    {{ log("sample: " ~ invocation_args_dict.get("sample"), info=true) }}
{% endif %}

SELECT * FROM {{ ref("input_model") }}

{% if is_incremental() %}
    WHERE event_time >= (SELECT max(event_time) FROM {{ this }})
{% endif %}
"""

snapshot_input_model_sql = """
{% snapshot snapshot_input_model %}
    {{ config(strategy='timestamp', unique_key='id', updated_at='event_time', event_time='event_time') }}

    select * from {{ ref('input_model') }}
{% endsnapshot %}
"""

model_from_snapshot_sql = """
{{ config(materialized='table') }}

SELECT * FROM {{ ref('snapshot_input_model') }}
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

    def drop_table(self, project, relation_name: str):
        relation = relation_from_name(project.adapter, "snapshot_input_model")
        project.run_sql(f"drop table if exists {relation}")


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
        "dbt_command,run_sample_mode,expected_row_count",
        [
            ("run", True, 2),
            ("run", False, 3),
            ("build", True, 2),
            ("build", False, 3),
        ],
    )
    @freezegun.freeze_time("2025-01-03T02:03:0Z")
    def test_sample_mode(
        self,
        project,
        event_catcher: EventCatcher,
        dbt_command: str,
        run_sample_mode: bool,
        expected_row_count: int,
    ):
        run_args = [dbt_command]
        expected_sample = None
        if run_sample_mode:
            run_args.append("--sample=1 day")
            expected_sample = SampleWindow(
                start=datetime(2025, 1, 2, 2, 3, 0, 0, tzinfo=pytz.UTC),
                end=datetime(2025, 1, 3, 2, 3, 0, 0, tzinfo=pytz.UTC),
            )

        _ = run_dbt(run_args, callbacks=[event_catcher.catch])
        assert len(event_catcher.caught_events) == 1
        assert event_catcher.caught_events[0].info.msg == f"Sample: {expected_sample}"  # type: ignore
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
        expected_filters = [
            "event_time >= '2025-01-01 02:03:00+00:00' and event_time < '2025-01-02 00:00:00+00:00'",
            "event_time >= '2025-01-02 00:00:00+00:00' and event_time < '2025-01-03 00:00:00+00:00'",
            "event_time >= '2025-01-03 00:00:00+00:00' and event_time < '2025-01-03 02:03:00+00:00'",
        ]

        _ = run_dbt(
            ["run", "--sample=2 day"],
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


class TestIncrementalModelSampleModeRelative(BaseSampleMode):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "input_model.sql": input_model_sql,
            "sample_incremental_merge.sql": sample_incremental_merge_sql,
        }

    @pytest.fixture
    def event_catcher(self) -> EventCatcher:
        return EventCatcher(event_to_catch=JinjaLogInfo, predicate=lambda event: "is_incremental: True" in event.info.msg)  # type: ignore

    @pytest.mark.parametrize(
        "sample,expected_rows",
        [
            (None, 6),
            ("3 days", 6),
            ("2 days", 5),
        ],
    )
    @freezegun.freeze_time("2025-01-06T18:03:0Z")
    def test_incremental_model_sample(
        self,
        project,
        event_catcher: EventCatcher,
        sample: Optional[str],
        expected_rows: int,
    ):
        # writing the input_model is necessary because we've parametrized the test
        # thus the "later_input_model" will still be present on the "non-first" runs
        write_file(input_model_sql, "models", "input_model.sql")

        # --full-refresh is necessary because we've parametrized the test
        _ = run_dbt(["run", "--full-refresh"], callbacks=[event_catcher.catch])

        assert len(event_catcher.caught_events) == 0
        self.assert_row_count(
            project=project,
            relation_name="sample_incremental_merge",
            expected_row_count=3,
        )

        # update the input file to have more rows
        write_file(later_input_model_sql, "models", "input_model.sql")

        run_args = ["run"]
        if sample is not None:
            run_args.extend([f"--sample={sample}"])

        _ = run_dbt(run_args, callbacks=[event_catcher.catch])

        assert len(event_catcher.caught_events) == 1
        self.assert_row_count(
            project=project,
            relation_name="sample_incremental_merge",
            expected_row_count=expected_rows,
        )


class TestIncrementalModelSampleModeSpecific(BaseSampleMode):
    # This had to be split out from the "relative" tests because `freezegun.freezetime`
    # breaks how timestamps get created.

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "input_model.sql": input_model_sql,
            "sample_incremental_merge.sql": sample_incremental_merge_sql,
        }

    @pytest.fixture
    def event_catcher(self) -> EventCatcher:
        return EventCatcher(event_to_catch=JinjaLogInfo, predicate=lambda event: "is_incremental: True" in event.info.msg)  # type: ignore

    @pytest.mark.parametrize(
        "sample,expected_rows",
        [
            (None, 6),
            ("{'start': '2025-01-03', 'end': '2025-01-07'}", 6),
            ("{'start': '2025-01-04', 'end': '2025-01-06'}", 5),
            ("{'start': '2025-01-05', 'end': '2025-01-07'}", 5),
            ("{'start': '2024-12-31', 'end': '2025-01-03'}", 3),
        ],
    )
    def test_incremental_model_sample(
        self,
        project,
        event_catcher: EventCatcher,
        sample: Optional[str],
        expected_rows: int,
    ):
        # writing the input_model is necessary because we've parametrized the test
        # thus the "later_input_model" will still be present on the "non-first" runs
        write_file(input_model_sql, "models", "input_model.sql")

        # --full-refresh is necessary because we've parametrized the test
        _ = run_dbt(["run", "--full-refresh"], callbacks=[event_catcher.catch])

        assert len(event_catcher.caught_events) == 0
        self.assert_row_count(
            project=project,
            relation_name="sample_incremental_merge",
            expected_row_count=3,
        )

        # update the input file to have more rows
        write_file(later_input_model_sql, "models", "input_model.sql")

        run_args = ["run"]
        if sample is not None:
            run_args.extend([f"--sample={sample}"])

        _ = run_dbt(run_args, callbacks=[event_catcher.catch])

        assert len(event_catcher.caught_events) == 1
        self.assert_row_count(
            project=project,
            relation_name="sample_incremental_merge",
            expected_row_count=expected_rows,
        )


class TestSampleSeedRefs(BaseSampleMode):
    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "input_seed.csv": input_seed_csv,
            "properties.yml": seed_properties_yml,
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "sample_input_seed.sql": sample_input_seed_sql,
        }

    @pytest.mark.parametrize(
        "run_sample_mode,expected_row_count",
        [
            (True, 2),
            (False, 3),
        ],
    )
    @freezegun.freeze_time("2025-01-03T02:03:0Z")
    def test_sample_mode(
        self,
        project,
        run_sample_mode: bool,
        expected_row_count: int,
    ):
        run_args = ["run"]
        if run_sample_mode:
            run_args.append("--sample=1 day")

        _ = run_dbt(["seed"])
        _ = run_dbt(run_args)
        self.assert_row_count(
            project=project,
            relation_name="sample_input_seed",
            expected_row_count=expected_row_count,
        )


class TestSamplingModelFromSnapshot(BaseSampleMode):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "input_model.sql": input_model_sql,
        }

    @pytest.fixture(scope="class")
    def snapshots(self):
        return {
            "snapshot_input_model.sql": snapshot_input_model_sql,
        }

    @pytest.mark.parametrize(
        "run_sample_mode,expected_row_count",
        [
            (True, 2),
            (False, 3),
        ],
    )
    @freezegun.freeze_time("2025-01-03T02:03:0Z")
    def test_sample_mode(
        self,
        project,
        run_sample_mode: bool,
        expected_row_count: int,
    ):
        run_args = ["build"]
        if run_sample_mode:
            run_args.append("--sample=1 day")

        _ = run_dbt(run_args)
        self.assert_row_count(
            project=project,
            relation_name="snapshot_input_model",
            expected_row_count=expected_row_count,
        )
        self.drop_table(project, "snapshot_input_model")


class TestSamplingSnapshot(BaseSampleMode):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "input_model.sql": input_model_sql,
        }

    @pytest.fixture(scope="class")
    def snapshots(self):
        return {
            "snapshot_input_model.sql": snapshot_input_model_sql,
        }

    @pytest.mark.parametrize(
        "run_sample_mode,expected_row_count",
        [
            (True, 2),
            (False, 3),
        ],
    )
    @freezegun.freeze_time("2025-01-03T02:03:0Z")
    def test_sample_mode(
        self,
        project,
        run_sample_mode: bool,
        expected_row_count: int,
    ):
        run_args = ["build"]

        # create the snapshot before building a model that depends on it
        _ = run_dbt(run_args)
        # Snapshot should always have 3 in this test because we don't sample it
        self.assert_row_count(
            project=project,
            relation_name="snapshot_input_model",
            expected_row_count=3,
        )

        if run_sample_mode:
            run_args.append("--sample=1 day")

        # create model that depends on the snapshot
        write_file(
            model_from_snapshot_sql, project.project_root, "models", "model_from_snapshot.sql"
        )

        _ = run_dbt(run_args)
        self.assert_row_count(
            project=project,
            relation_name="model_from_snapshot",
            expected_row_count=expected_row_count,
        )
        self.drop_table(project, "snapshot_input_model")
