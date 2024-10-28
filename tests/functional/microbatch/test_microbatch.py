import os
from datetime import datetime
from unittest import mock

import pytest
import pytz

from dbt.events.types import LogModelResult
from dbt.tests.util import (
    get_artifact,
    patch_microbatch_end_time,
    read_file,
    relation_from_name,
    run_dbt,
    run_dbt_and_capture,
    write_file,
)
from tests.utils import EventCatcher

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
{{ config(materialized='incremental', incremental_strategy='microbatch', unique_key='id', event_time='event_time', batch_size='day', begin=modules.datetime.datetime(2020, 1, 1, 0, 0, 0)) }}
select * from {{ ref('input_model') }}
"""

invalid_batch_context_macro_sql = """
{% macro check_invalid_batch_context() %}

{% if model is not mapping %}
    {{ exceptions.raise_compiler_error("`model` is invalid: expected mapping type") }}
{% elif compiled_code and compiled_code is not string %}
    {{ exceptions.raise_compiler_error("`compiled_code` is invalid: expected string type") }}
{% elif sql and sql is not string %}
    {{ exceptions.raise_compiler_error("`sql` is invalid: expected string type") }}
{% elif is_incremental is not callable %}
    {{ exceptions.raise_compiler_error("`is_incremental()` is invalid: expected callable type") }}
{% elif should_full_refresh is not callable %}
    {{ exceptions.raise_compiler_error("`should_full_refresh()` is invalid: expected callable type") }}
{% endif %}

{% endmacro %}
"""

microbatch_model_with_context_checks_sql = """
{{ config(pre_hook="{{ check_invalid_batch_context() }}", materialized='incremental', incremental_strategy='microbatch', unique_key='id', event_time='event_time', batch_size='day', begin=modules.datetime.datetime(2020, 1, 1, 0, 0, 0)) }}

{{ check_invalid_batch_context() }}
select * from {{ ref('input_model') }}
"""

microbatch_model_downstream_sql = """
{{ config(materialized='incremental', incremental_strategy='microbatch', unique_key='id', event_time='event_time', batch_size='day', begin=modules.datetime.datetime(2020, 1, 1, 0, 0, 0)) }}
select * from {{ ref('microbatch_model') }}
"""


microbatch_model_ref_render_sql = """
{{ config(materialized='incremental', incremental_strategy='microbatch', unique_key='id', event_time='event_time', batch_size='day', begin=modules.datetime.datetime(2020, 1, 1, 0, 0, 0)) }}
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
{{ config(materialized='incremental', incremental_strategy='microbatch', unique_key='id', event_time='event_time', batch_size='day', begin=modules.datetime.datetime(2020, 1, 1, 0, 0, 0)) }}
select * from {{ source('seed_sources', 'raw_source') }}
"""

custom_microbatch_strategy = """
{% macro get_incremental_microbatch_sql(arg_dict) %}
    {% do log('custom microbatch strategy', info=True) %}

     {%- set dest_cols_csv = get_quoted_csv(arg_dict["dest_columns"] | map(attribute="name")) -%}

    insert into {{ arg_dict["target_relation"] }} ({{ dest_cols_csv }})
    (
        select {{ dest_cols_csv }}
        from {{ arg_dict["temp_relation"] }}
    )

{% endmacro %}
"""


downstream_model_of_microbatch_sql = """
SELECT * FROM {{ ref('microbatch_model') }}
"""

microbatch_model_full_refresh_false_sql = """
{{ config(materialized='incremental', incremental_strategy='microbatch', unique_key='id', event_time='event_time', batch_size='day', begin=modules.datetime.datetime(2020, 1, 1, 0, 0, 0), full_refresh=False) }}
select * from {{ ref('input_model') }}
"""


class BaseMicrobatchCustomUserStrategy:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "input_model.sql": input_model_sql,
            "microbatch_model.sql": microbatch_model_sql,
        }

    @pytest.fixture(scope="class")
    def macros(self):
        return {"microbatch.sql": custom_microbatch_strategy}


class TestMicrobatchCustomUserStrategyDefault(BaseMicrobatchCustomUserStrategy):
    def test_use_custom_microbatch_strategy_by_default(self, project):
        with mock.patch.object(
            type(project.adapter), "valid_incremental_strategies", lambda _: []
        ):
            # Initial run
            run_dbt(["run"])

            # Incremental run uses custom strategy
            _, logs = run_dbt_and_capture(["run"])
            assert "custom microbatch strategy" in logs


class TestMicrobatchCustomUserStrategyEnvVarTrueValid(BaseMicrobatchCustomUserStrategy):
    @mock.patch.dict(os.environ, {"DBT_EXPERIMENTAL_MICROBATCH": "True"})
    def test_use_custom_microbatch_strategy_env_var_true_invalid_incremental_strategy(
        self, project
    ):
        with mock.patch.object(
            type(project.adapter), "valid_incremental_strategies", lambda _: ["microbatch"]
        ):
            # Initial run
            with patch_microbatch_end_time("2020-01-03 13:57:00"):
                run_dbt(["run"])

            # Incremental run uses custom strategy
            with patch_microbatch_end_time("2020-01-03 13:57:00"):
                _, logs = run_dbt_and_capture(["run"])
            assert "custom microbatch strategy" in logs


# TODO: Consider a behaviour flag here if DBT_EXPERIMENTAL_MICROBATCH is removed
# Since this causes an exception prior to using an override
class TestMicrobatchCustomUserStrategyEnvVarTrueInvalid(BaseMicrobatchCustomUserStrategy):
    @mock.patch.dict(os.environ, {"DBT_EXPERIMENTAL_MICROBATCH": "True"})
    def test_use_custom_microbatch_strategy_env_var_true_invalid_incremental_strategy(
        self, project
    ):
        with mock.patch.object(
            type(project.adapter), "valid_incremental_strategies", lambda _: []
        ):
            # Run of microbatch model while adapter doesn't have a "valid"
            # microbatch strategy causes an error to be raised
            with patch_microbatch_end_time("2020-01-03 13:57:00"):
                _, logs = run_dbt_and_capture(["run"], expect_pass=False)
            assert "'microbatch' is not valid" in logs


class BaseMicrobatchTest:
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


class TestMicrobatchCLI(BaseMicrobatchTest):
    @mock.patch.dict(os.environ, {"DBT_EXPERIMENTAL_MICROBATCH": "True"})
    def test_run_with_event_time(self, project):
        # run without --event-time-start or --event-time-end - 3 expected rows in output
        catcher = EventCatcher(event_to_catch=LogModelResult)

        with patch_microbatch_end_time("2020-01-03 13:57:00"):
            run_dbt(["run"], callbacks=[catcher.catch])
        self.assert_row_count(project, "microbatch_model", 3)

        assert len(catcher.caught_events) == 5
        batch_creation_events = 0
        for caught_event in catcher.caught_events:
            if "batch 2020" in caught_event.data.description:
                batch_creation_events += 1
                assert caught_event.data.execution_time > 0
        # 3 batches should have been run, so there should be 3 batch
        # creation events
        assert batch_creation_events == 3

        # build model >= 2020-01-02
        with patch_microbatch_end_time("2020-01-03 13:57:00"):
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


class TestMicroBatchBoundsDefault(BaseMicrobatchTest):
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


class TestMicrobatchWithSource(BaseMicrobatchTest):
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


class TestMicrobatchJinjaContext(BaseMicrobatchTest):

    @pytest.fixture(scope="class")
    def macros(self):
        return {"check_batch_context.sql": invalid_batch_context_macro_sql}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "input_model.sql": input_model_sql,
            "microbatch_model.sql": microbatch_model_with_context_checks_sql,
        }

    @mock.patch.dict(os.environ, {"DBT_EXPERIMENTAL_MICROBATCH": "True"})
    def test_run_with_event_time(self, project):
        # initial run -- backfills all data
        with patch_microbatch_end_time("2020-01-03 13:57:00"):
            run_dbt(["run"])
        self.assert_row_count(project, "microbatch_model", 3)


class TestMicrobatchWithInputWithoutEventTime(BaseMicrobatchTest):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "input_model.sql": input_model_without_event_time_sql,
            "microbatch_model.sql": microbatch_model_sql,
        }

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


class TestMicrobatchUsingRefRenderSkipsFilter(BaseMicrobatchTest):
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


microbatch_model_context_vars = """
{{ config(materialized='incremental', incremental_strategy='microbatch', unique_key='id', event_time='event_time', batch_size='day', begin=modules.datetime.datetime(2020, 1, 1, 0, 0, 0)) }}
{{ log("start: "~ model.config.__dbt_internal_microbatch_event_time_start, info=True)}}
{{ log("end: "~ model.config.__dbt_internal_microbatch_event_time_end, info=True)}}
select * from {{ ref('input_model') }}
"""


class TestMicrobatchJinjaContextVarsAvailable(BaseMicrobatchTest):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "input_model.sql": input_model_sql,
            "microbatch_model.sql": microbatch_model_context_vars,
        }

    @mock.patch.dict(os.environ, {"DBT_EXPERIMENTAL_MICROBATCH": "True"})
    def test_run_with_event_time_logs(self, project):
        with patch_microbatch_end_time("2020-01-03 13:57:00"):
            _, logs = run_dbt_and_capture(["run", "--event-time-start", "2020-01-01"])

        assert "start: 2020-01-01 00:00:00+00:00" in logs
        assert "end: 2020-01-02 00:00:00+00:00" in logs

        assert "start: 2020-01-02 00:00:00+00:00" in logs
        assert "end: 2020-01-03 00:00:00+00:00" in logs

        assert "start: 2020-01-03 00:00:00+00:00" in logs
        assert "end: 2020-01-03 13:57:00+00:00" in logs


microbatch_model_failing_incremental_partition_sql = """
{{ config(materialized='incremental', incremental_strategy='microbatch', unique_key='id', event_time='event_time', batch_size='day', begin=modules.datetime.datetime(2020, 1, 1, 0, 0, 0)) }}
{% if '2020-01-02' in (model.config.__dbt_internal_microbatch_event_time_start | string) %}
 invalid_sql
{% endif %}
select * from {{ ref('input_model') }}
"""


class TestMicrobatchIncrementalPartitionFailure(BaseMicrobatchTest):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "input_model.sql": input_model_sql,
            "microbatch_model.sql": microbatch_model_failing_incremental_partition_sql,
            "downstream_model.sql": downstream_model_of_microbatch_sql,
        }

    @mock.patch.dict(os.environ, {"DBT_EXPERIMENTAL_MICROBATCH": "True"})
    def test_run_with_event_time(self, project):
        # run all partitions from start - 2 expected rows in output, one failed
        with patch_microbatch_end_time("2020-01-03 13:57:00"):
            run_dbt(["run", "--event-time-start", "2020-01-01"], expect_pass=False)
        self.assert_row_count(project, "microbatch_model", 2)

        run_results = get_artifact(project.project_root, "target", "run_results.json")
        microbatch_run_result = run_results["results"][1]
        assert microbatch_run_result["status"] == "partial success"
        batch_results = microbatch_run_result["batch_results"]
        assert batch_results is not None
        assert len(batch_results["successful"]) == 2
        assert len(batch_results["failed"]) == 1
        assert run_results["results"][2]["status"] == "skipped"


class TestMicrobatchRetriesPartialSuccesses(BaseMicrobatchTest):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "input_model.sql": input_model_sql,
            "microbatch_model.sql": microbatch_model_failing_incremental_partition_sql,
        }

    @mock.patch.dict(os.environ, {"DBT_EXPERIMENTAL_MICROBATCH": "True"})
    def test_run_with_event_time(self, project):
        # run all partitions from start - 2 expected rows in output, one failed
        with patch_microbatch_end_time("2020-01-03 13:57:00"):
            _, console_output = run_dbt_and_capture(["run", "--event-time-start", "2020-01-01"])

        assert "PARTIAL SUCCESS (2/3)" in console_output
        assert "Completed with 1 partial success" in console_output

        self.assert_row_count(project, "microbatch_model", 2)

        run_results = get_artifact(project.project_root, "target", "run_results.json")
        microbatch_run_result = run_results["results"][1]
        assert microbatch_run_result["status"] == "partial success"
        batch_results = microbatch_run_result["batch_results"]
        assert batch_results is not None
        assert len(batch_results["successful"]) == 2
        assert len(batch_results["failed"]) == 1

        # update the microbatch model so that it no longer fails
        write_file(microbatch_model_sql, project.project_root, "models", "microbatch_model.sql")

        with patch_microbatch_end_time("2020-01-03 13:57:00"):
            _, console_output = run_dbt_and_capture(["retry"])

        assert "PARTIAL SUCCESS" not in console_output
        assert "Completed with 1 partial success" not in console_output
        assert "Completed successfully" in console_output

        self.assert_row_count(project, "microbatch_model", 3)


class TestMicrobatchMultipleRetries(BaseMicrobatchTest):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "input_model.sql": input_model_sql,
            "microbatch_model.sql": microbatch_model_failing_incremental_partition_sql,
        }

    @mock.patch.dict(os.environ, {"DBT_EXPERIMENTAL_MICROBATCH": "True"})
    def test_run_with_event_time(self, project):
        # run all partitions from start - 2 expected rows in output, one failed
        with patch_microbatch_end_time("2020-01-03 13:57:00"):
            _, console_output = run_dbt_and_capture(["run", "--event-time-start", "2020-01-01"])

        assert "PARTIAL SUCCESS (2/3)" in console_output
        assert "Completed with 1 partial success" in console_output

        self.assert_row_count(project, "microbatch_model", 2)

        with patch_microbatch_end_time("2020-01-03 13:57:00"):
            _, console_output = run_dbt_and_capture(["retry"], expect_pass=False)

        assert "PARTIAL SUCCESS" not in console_output
        assert "ERROR" in console_output
        assert "Completed with 1 error, 0 partial successs, and 0 warnings" in console_output

        self.assert_row_count(project, "microbatch_model", 2)

        with patch_microbatch_end_time("2020-01-03 13:57:00"):
            _, console_output = run_dbt_and_capture(["retry"], expect_pass=False)

        assert "PARTIAL SUCCESS" not in console_output
        assert "ERROR" in console_output
        assert "Completed with 1 error, 0 partial successs, and 0 warnings" in console_output

        self.assert_row_count(project, "microbatch_model", 2)


microbatch_model_first_partition_failing_sql = """
{{ config(materialized='incremental', incremental_strategy='microbatch', unique_key='id', event_time='event_time', batch_size='day', begin=modules.datetime.datetime(2020, 1, 1, 0, 0, 0)) }}
{% if '2020-01-01' in (model.config.__dbt_internal_microbatch_event_time_start | string) %}
 invalid_sql
{% endif %}
select * from {{ ref('input_model') }}
"""


class TestMicrobatchInitialPartitionFailure(BaseMicrobatchTest):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "input_model.sql": input_model_sql,
            "microbatch_model.sql": microbatch_model_first_partition_failing_sql,
        }

    @mock.patch.dict(os.environ, {"DBT_EXPERIMENTAL_MICROBATCH": "True"})
    def test_run_with_event_time(self, project):
        # run all partitions from start - 2 expected rows in output, one failed
        with patch_microbatch_end_time("2020-01-03 13:57:00"):
            run_dbt(["run", "--event-time-start", "2020-01-01"])
        self.assert_row_count(project, "microbatch_model", 2)


class TestMicrobatchCompiledRunPaths(BaseMicrobatchTest):
    @mock.patch.dict(os.environ, {"DBT_EXPERIMENTAL_MICROBATCH": "True"})
    def test_run_with_event_time(self, project):
        # run all partitions from start - 2 expected rows in output, one failed
        with patch_microbatch_end_time("2020-01-03 13:57:00"):
            run_dbt(["run", "--event-time-start", "2020-01-01"])

        # Compiled paths - compiled model without filter only
        assert read_file(
            project.project_root,
            "target",
            "compiled",
            "test",
            "models",
            "microbatch_model.sql",
        )

        # Compiled paths - batch compilations
        assert read_file(
            project.project_root,
            "target",
            "compiled",
            "test",
            "models",
            "microbatch_model",
            "microbatch_model_2020-01-01.sql",
        )
        assert read_file(
            project.project_root,
            "target",
            "compiled",
            "test",
            "models",
            "microbatch_model",
            "microbatch_model_2020-01-02.sql",
        )
        assert read_file(
            project.project_root,
            "target",
            "compiled",
            "test",
            "models",
            "microbatch_model",
            "microbatch_model_2020-01-03.sql",
        )

        assert read_file(
            project.project_root,
            "target",
            "run",
            "test",
            "models",
            "microbatch_model",
            "microbatch_model_2020-01-01.sql",
        )
        assert read_file(
            project.project_root,
            "target",
            "run",
            "test",
            "models",
            "microbatch_model",
            "microbatch_model_2020-01-02.sql",
        )
        assert read_file(
            project.project_root,
            "target",
            "run",
            "test",
            "models",
            "microbatch_model",
            "microbatch_model_2020-01-03.sql",
        )


class TestMicrobatchFullRefreshConfigFalse(BaseMicrobatchTest):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "input_model.sql": input_model_sql,
            "microbatch_model.sql": microbatch_model_full_refresh_false_sql,
            "downstream_model.sql": downstream_model_of_microbatch_sql,
        }

    @mock.patch.dict(os.environ, {"DBT_EXPERIMENTAL_MICROBATCH": "True"})
    def test_run_with_event_time(self, project):
        # run all partitions from 2020-01-02 to spoofed "now" - 2 expected rows in output
        with patch_microbatch_end_time("2020-01-03 13:57:00"):
            run_dbt(["run", "--event-time-start", "2020-01-02"])
        self.assert_row_count(project, "microbatch_model", 2)

        # re-running shouldn't change what it's in the data set because there is nothing new
        with patch_microbatch_end_time("2020-01-03 13:57:00"):
            run_dbt(["run"])
        self.assert_row_count(project, "microbatch_model", 2)

        # running with --full-refresh shouldn't pick up 2020-01-01 BECAUSE the model has
        # full_refresh = false
        with patch_microbatch_end_time("2020-01-03 13:57:00"):
            run_dbt(["run", "--full-refresh"])
        self.assert_row_count(project, "microbatch_model", 2)

        # update the microbatch model to no longer have full_refresh=False config
        write_file(microbatch_model_sql, project.project_root, "models", "microbatch_model.sql")

        # running with full refresh should now pick up the 2020-01-01 data
        with patch_microbatch_end_time("2020-01-03 13:57:00"):
            run_dbt(["run", "--full-refresh"])
        self.assert_row_count(project, "microbatch_model", 3)


class TestMicrbobatchModelsRunWithSameCurrentTime(BaseMicrobatchTest):

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "input_model.sql": input_model_sql,
            "microbatch_model.sql": microbatch_model_sql,
            "second_microbatch_model.sql": microbatch_model_downstream_sql,
        }

    @mock.patch.dict(os.environ, {"DBT_EXPERIMENTAL_MICROBATCH": "True"})
    def test_microbatch(self, project) -> None:
        current_time = datetime.now(pytz.UTC)
        run_dbt(["run", "--event-time-start", current_time.strftime("%Y-%m-%d")])

        run_results = get_artifact(project.project_root, "target", "run_results.json")
        microbatch_model_last_batch = run_results["results"][1]["batch_results"]["successful"][-1]
        second_microbatch_model_last_batch = run_results["results"][2]["batch_results"][
            "successful"
        ][-1]

        # they should have the same last batch because they are using the _same_ "current_time"
        assert microbatch_model_last_batch == second_microbatch_model_last_batch
