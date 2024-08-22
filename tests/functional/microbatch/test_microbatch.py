import pytest

from dbt.tests.util import relation_from_name, run_dbt

input_model_sql = """
{{ config(event_time='event_time') }}

select 1 as id, DATE '2020-01-01' as event_time, 'invalid' as status
union all
select 2 as id, DATE '2020-01-02' as event_time, 'success' as status
union all
select 3 as id, DATE '2020-01-03' as event_time, 'failed' as status
"""

microbatch_model_sql = """
{{ config(materialized='incremental', strategy='microbatch', event_time='event_time') }}
select * from {{ ref('input_model') }}
"""

microbatch_model_yml = """
models:
  - name: microbatch_model
    columns:
      - name: status
        tests:
          - accepted_values:
              values: ['success', 'failed']
"""


class TestMicrobatch:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "input_model.sql": input_model_sql,
            "microbatch_model.sql": microbatch_model_sql,
            "microbatch.yml": microbatch_model_yml,
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
