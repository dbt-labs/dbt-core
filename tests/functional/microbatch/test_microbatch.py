import pytest
from freezegun import freeze_time

from dbt.tests.util import relation_from_name, run_dbt

input_model_sql = """
{{ config(event_time='event_time') }}

select 1 as id, DATE '2020-01-01' as event_time
union all
select 2 as id, DATE '2020-01-02' as event_time
union all
select 3 as id, DATE '2020-01-03' as event_time
"""

microbatch_model_sql = """
{{ config(materialized='incremental', event_time='event_time', partition_grain='day') }}
select * from {{ ref('input_model') }}
"""


class TestMicrobatchCLI:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "input_model.sql": input_model_sql,
            "microbatch_model.sql": microbatch_model_sql,
        }

    def assert_row_count(self, project, relation_name: str, expected_row_count: int):
        relation = relation_from_name(project.adapter, relation_name)
        result = project.run_sql(f"select count(*) as num_rows from {relation}", fetch="one")

        if result[0] != expected_row_count:
            # running show for debugging
            run_dbt(["show", "--inline", f"select * from {relation}"])

            assert result[0] == expected_row_count

    def test_run_with_event_time(self, project):
        # run without --event-time-start or --event-time-end - 3 expected rows in output
        run_dbt(["run"])
        self.assert_row_count(project, "microbatch_model", 3)

        # build model >= 2020-01-02
        run_dbt(["run", "--event-time-start", "2020-01-02", "--full-refresh"])
        self.assert_row_count(project, "microbatch_model", 2)

        # build model < 2020-01-03
        run_dbt(["run", "--event-time-end", "2020-01-03", "--full-refresh"])
        self.assert_row_count(project, "microbatch_model", 2)

        # build model between 2020-01-02 >= event_time < 2020-01-03
        run_dbt(
            [
                "run",
                "--event-time-start",
                "2020-01-02",
                "--event-time-end",
                "2020-01-03",
                "--full-refresh",
            ]
        )
        self.assert_row_count(project, "microbatch_model", 1)

        # results = run_dbt(["test", "--select", "microbatch_model", "--event-time-start", "2020-05-01", "--event-time-end", "2020-05-03"])


class TestMicroBatchBoundsDefault:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "input_model.sql": input_model_sql,
            "microbatch_model.sql": microbatch_model_sql,
        }

    def assert_row_count(self, project, relation_name: str, expected_row_count: int):
        relation = relation_from_name(project.adapter, relation_name)
        result = project.run_sql(f"select count(*) as num_rows from {relation}", fetch="one")

        if result[0] != expected_row_count:
            # running show for debugging
            run_dbt(["show", "--inline", f"select * from {relation}"])

            assert result[0] == expected_row_count

    def test_run_with_event_time(self, project):
        # initial run
        with freeze_time("2020-01-01 13:57:00"):
            run_dbt(["run"])
        self.assert_row_count(project, "microbatch_model", 1)

        # our partition grain is "day" so running the same day without new data should produce the same results
        with freeze_time("2020-01-03 14:57:00"):
            run_dbt(["run"])
        self.assert_row_count(project, "microbatch_model", 3)

        # results = run_dbt(["test", "--select", "microbatch_model", "--event-time-start", "2020-05-01", "--event-time-end", "2020-05-03"])
