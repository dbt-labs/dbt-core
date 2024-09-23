import pytest
from dbt.tests.util import (
    patch_microbatch_end_time,
    relation_from_name,
    run_dbt,
    run_dbt_and_capture,
    write_file,
)
from dbt.exceptions import ParsingError

missing_event_time_microbatch_model_sql = """
{{ config(materialized='incremental', incremental_strategy='microbatch', batch_size='day') }}
select * from {{ ref('input_model') }}
"""

invalid_event_time_microbatch_model_sql = """
{{ config(materialized='incremental', incremental_strategy='microbatch', batch_size='day', event_time=2) }}
select * from {{ ref('input_model') }}
"""

missing_batch_size_microbatch_model_sql = """
{{ config(materialized='incremental', incremental_strategy='microbatch', event_time='event_time') }}
select * from {{ ref('input_model') }}
"""

invalid_batch_size_microbatch_model_sql = """
{{ config(materialized='incremental', incremental_strategy='microbatch', batch_size='invalid', event_time='event_time') }}
select * from {{ ref('input_model') }}
"""

invalid_event_time_input_model_sql = """
{{ config(materialized='table', event_time=1) }}

select 1 as id, TIMESTAMP '2020-01-01 00:00:00-0' as event_time
"""

valid_input_model_sql = """
{{ config(materialized='table') }}

select 1 as id, TIMESTAMP '2020-01-01 00:00:00-0' as event_time
"""


class BaseMicrobatchTest:
    @pytest.fixture(scope="class")
    def models(self):
        return {}

    def test_parsing_error_raised(self, project):
        with pytest.raises(ParsingError):
            run_dbt(["parse"])


class TestMissingEventTimeMicrobatch(BaseMicrobatchTest):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "input_model.sql": valid_input_model_sql,
            "microbatch.sql": missing_event_time_microbatch_model_sql
        }
