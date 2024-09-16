import os
from unittest import mock

import pytest

from dbt.tests.util import (
    patch_microbatch_end_time,
    relation_from_name,
    run_dbt,
    write_file,
)

input_model_sql = """
{{ config(materialized='table', event_time='event_time') }}

select 1 as id, TIMESTAMP '2020-01-01 00:00:00-0' as event_time
union all
select 2 as id, TIMESTAMP '2020-01-02 00:00:00-0' as event_time
union all
select 3 as id, TIMESTAMP '2020-01-03 00:00:00-0' as event_time
"""

input_model_without_event_time_sql = """
{{ config(materialized='table') }}

select 1 as id, TIMESTAMP '2020-01-01 00:00:00-0' as event_time
union all
select 2 as id, TIMESTAMP '2020-01-02 00:00:00-0' as event_time
union all
select 3 as id, TIMESTAMP '2020-01-03 00:00:00-0' as event_time
"""

microbatch_model_sql = """
{{ config(materialized='incremental', incremental_strategy='microbatch', unique_key='id', event_time='event_time', batch_size='day') }}
select * from {{ ref('input_model') }}
"""

microbatch_model_ref_render_sql = """
{{ config(materialized='incremental', incremental_strategy='microbatch', unique_key='id', event_time='event_time', batch_size='day') }}
select * from {{ ref('input_model').render() }}
"""

seed_csv = """id,event_time
1,'2020-01-01 00:00:00-0'
2,'2020-01-02 00:00:00-0'
3,'2020-01-03 00:00:00-0'
"""

seeds_yaml = """
seeds:
  - name: raw_source
    config:
      column_types:
        event_time: TIMESTAMP
"""

sources_yaml = """
sources:
  - name: seed_sources
    schema: "{{ target.schema }}"
    tables:
      - name: raw_source
        config:
          event_time: event_time
"""

microbatch_model_calling_source_sql = """
{{ config(materialized='incremental', incremental_strategy='microbatch', unique_key='id', event_time='event_time', batch_size='day') }}
select * from {{ source('seed_sources', 'raw_source') }}
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

    @mock.patch.dict(os.environ, {"DBT_EXPERIMENTAL_MICROBATCH": "True"})
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

    @mock.patch.dict(os.environ, {"DBT_EXPERIMENTAL_MICROBATCH": "True"})
    def test_run_with_event_time(self, project):
        # initial run -- backfills all data
        with patch_microbatch_end_time("2020-01-03 13:57:00"):
            run_dbt(["run"])
        self.assert_row_count(project, "microbatch_model", 3)

        # our partition grain is "day" so running the same day without new data should produce the same results
        with patch_microbatch_end_time("2020-01-03 14:57:00"):
            run_dbt(["run"])
        self.assert_row_count(project, "microbatch_model", 3)

        # add next two days of data
        test_schema_relation = project.adapter.Relation.create(
            database=project.database, schema=project.test_schema
        )
        project.run_sql(
            f"insert into {test_schema_relation}.input_model(id, event_time) values (4, TIMESTAMP '2020-01-04 00:00:00-0'), (5, TIMESTAMP '2020-01-05 00:00:00-0')"
        )
        self.assert_row_count(project, "input_model", 5)

        # re-run without changing current time => no insert
        with patch_microbatch_end_time("2020-01-03 14:57:00"):
            run_dbt(["run", "--select", "microbatch_model"])
        self.assert_row_count(project, "microbatch_model", 3)

        # re-run by advancing time by one day changing current time => insert 1 row
        with patch_microbatch_end_time("2020-01-04 14:57:00"):
            run_dbt(["run", "--select", "microbatch_model"])
        self.assert_row_count(project, "microbatch_model", 4)

        # re-run by advancing time by one more day changing current time => insert 1 more row
        with patch_microbatch_end_time("2020-01-05 14:57:00"):
            run_dbt(["run", "--select", "microbatch_model"])
        self.assert_row_count(project, "microbatch_model", 5)


class TestMicrobatchWithSource:
    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "raw_source.csv": seed_csv,
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "microbatch_model.sql": microbatch_model_calling_source_sql,
            "sources.yml": sources_yaml,
            "seeds.yml": seeds_yaml,
        }

    def assert_row_count(self, project, relation_name: str, expected_row_count: int):
        relation = relation_from_name(project.adapter, relation_name)
        result = project.run_sql(f"select count(*) as num_rows from {relation}", fetch="one")

        if result[0] != expected_row_count:
            # running show for debugging
            run_dbt(["show", "--inline", f"select * from {relation}"])

            assert result[0] == expected_row_count

    @mock.patch.dict(os.environ, {"DBT_EXPERIMENTAL_MICROBATCH": "True"})
    def test_run_with_event_time(self, project):
        # ensure seed is created for source
        run_dbt(["seed"])

        # initial run -- backfills all data
        with patch_microbatch_end_time("2020-01-03 13:57:00"):
            run_dbt(["run"])
        self.assert_row_count(project, "microbatch_model", 3)

        # our partition grain is "day" so running the same day without new data should produce the same results
        with patch_microbatch_end_time("2020-01-03 14:57:00"):
            run_dbt(["run"])
        self.assert_row_count(project, "microbatch_model", 3)

        # add next two days of data
        test_schema_relation = project.adapter.Relation.create(
            database=project.database, schema=project.test_schema
        )
        project.run_sql(
            f"insert into {test_schema_relation}.raw_source(id, event_time) values (4, TIMESTAMP '2020-01-04 00:00:00-0'), (5, TIMESTAMP '2020-01-05 00:00:00-0')"
        )
        self.assert_row_count(project, "raw_source", 5)

        # re-run without changing current time => no insert
        with patch_microbatch_end_time("2020-01-03 14:57:00"):
            run_dbt(["run", "--select", "microbatch_model"])
        self.assert_row_count(project, "microbatch_model", 3)

        # re-run by advancing time by one day changing current time => insert 1 row
        with patch_microbatch_end_time("2020-01-04 14:57:00"):
            run_dbt(["run", "--select", "microbatch_model"])
        self.assert_row_count(project, "microbatch_model", 4)

        # re-run by advancing time by one more day changing current time => insert 1 more row
        with patch_microbatch_end_time("2020-01-05 14:57:00"):
            run_dbt(["run", "--select", "microbatch_model"])
        self.assert_row_count(project, "microbatch_model", 5)


class TestMicrobatchWithInputWithoutEventTime:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "input_model.sql": input_model_without_event_time_sql,
            "microbatch_model.sql": microbatch_model_sql,
        }

    def assert_row_count(self, project, relation_name: str, expected_row_count: int):
        relation = relation_from_name(project.adapter, relation_name)
        result = project.run_sql(f"select count(*) as num_rows from {relation}", fetch="one")

        if result[0] != expected_row_count:
            # running show for debugging
            run_dbt(["show", "--inline", f"select * from {relation}"])

            assert result[0] == expected_row_count

    @mock.patch.dict(os.environ, {"DBT_EXPERIMENTAL_MICROBATCH": "True"})
    def test_run_with_event_time(self, project):
        # initial run -- backfills all data
        with patch_microbatch_end_time("2020-01-03 13:57:00"):
            run_dbt(["run"])
        self.assert_row_count(project, "microbatch_model", 3)

        # our partition grain is "day" so running the same day without new data should produce the same results
        with patch_microbatch_end_time("2020-01-03 14:57:00"):
            run_dbt(["run"])
        self.assert_row_count(project, "microbatch_model", 3)

        # add next two days of data
        test_schema_relation = project.adapter.Relation.create(
            database=project.database, schema=project.test_schema
        )
        project.run_sql(
            f"insert into {test_schema_relation}.input_model(id, event_time) values (4, TIMESTAMP '2020-01-04 00:00:00-0'), (5, TIMESTAMP '2020-01-05 00:00:00-0')"
        )
        self.assert_row_count(project, "input_model", 5)

        # re-run without changing current time => INSERT BECAUSE INPUT MODEL ISN'T BEING FILTERED
        with patch_microbatch_end_time("2020-01-03 14:57:00"):
            run_dbt(["run", "--select", "microbatch_model"])
        self.assert_row_count(project, "microbatch_model", 5)


class TestMicrobatchUsingRefRenderSkipsFilter:
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

    @mock.patch.dict(os.environ, {"DBT_EXPERIMENTAL_MICROBATCH": "True"})
    def test_run_with_event_time(self, project):
        # initial run -- backfills all data
        with patch_microbatch_end_time("2020-01-03 13:57:00"):
            run_dbt(["run"])
        self.assert_row_count(project, "microbatch_model", 3)

        # our partition grain is "day" so running the same day without new data should produce the same results
        with patch_microbatch_end_time("2020-01-03 14:57:00"):
            run_dbt(["run"])
        self.assert_row_count(project, "microbatch_model", 3)

        # add next two days of data
        test_schema_relation = project.adapter.Relation.create(
            database=project.database, schema=project.test_schema
        )
        project.run_sql(
            f"insert into {test_schema_relation}.input_model(id, event_time) values (4, TIMESTAMP '2020-01-04 00:00:00-0'), (5, TIMESTAMP '2020-01-05 00:00:00-0')"
        )
        self.assert_row_count(project, "input_model", 5)

        # re-run without changing current time => no insert
        with patch_microbatch_end_time("2020-01-03 14:57:00"):
            run_dbt(["run", "--select", "microbatch_model"])
        self.assert_row_count(project, "microbatch_model", 3)

        # Update microbatch model to call .render() on ref('input_model')
        write_file(
            microbatch_model_ref_render_sql, project.project_root, "models", "microbatch_model.sql"
        )

        # re-run without changing current time => INSERT because .render() skips filtering
        with patch_microbatch_end_time("2020-01-03 14:57:00"):
            run_dbt(["run", "--select", "microbatch_model"])
        self.assert_row_count(project, "microbatch_model", 5)
