import pytest

from dbt.exceptions import ParsingError
from dbt.tests.util import run_dbt

valid_microbatch_model_sql = """
{{ config(materialized='incremental', incremental_strategy='microbatch', batch_size='day', event_time='event_time') }}
select * from {{ ref('input_model') }}
"""

valid_microbatch_model_no_config_sql = """
select * from {{ ref('input_model') }}
"""

valid_microbatch_model_config_yml = """
models:
  - name: microbatch
    config:
      materialized: incremental
      incremental_strategy: microbatch
      batch_size: day
      event_time: event_time
      begin: 2020-01-01
"""

invalid_microbatch_model_config_yml = """
models:
  - name: microbatch
    config:
      materialized: incremental
      incremental_strategy: microbatch
      batch_size: day
      event_time: event_time
      begin: 2020-01-01 11 PM
"""

missing_event_time_microbatch_model_sql = """
{{ config(materialized='incremental', incremental_strategy='microbatch', batch_size='day') }}
select * from {{ ref('input_model') }}
"""

invalid_event_time_microbatch_model_sql = """
{{ config(materialized='incremental', incremental_strategy='microbatch', batch_size='day', event_time=2) }}
select * from {{ ref('input_model') }}
"""

missing_begin_microbatch_model_sql = """
{{ config(materialized='incremental', incremental_strategy='microbatch', batch_size='day', event_time='event_time') }}
select * from {{ ref('input_model') }}
"""

invalid_begin_microbatch_model_sql = """
{{ config(materialized='incremental', incremental_strategy='microbatch', batch_size='day', event_time='event_time', begin=2) }}
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


class BaseMicrobatchTestParseError:
    @pytest.fixture(scope="class")
    def models(self):
        return {}

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "flags": {
                "require_batched_execution_for_custom_microbatch_strategy": True,
            }
        }

    def test_parsing_error_raised(self, project):
        with pytest.raises(ParsingError):
            run_dbt(["parse"])


class BaseMicrobatchTestNoError:
    @pytest.fixture(scope="class")
    def models(self):
        return {}

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "flags": {
                "require_batched_execution_for_custom_microbatch_strategy": True,
            }
        }

    def test_parsing_error_not_raised(self, project):
        run_dbt(["parse"])


class TestMissingEventTimeMicrobatch(BaseMicrobatchTestParseError):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "input_model.sql": valid_input_model_sql,
            "microbatch.sql": missing_event_time_microbatch_model_sql,
        }


class TestInvalidEventTimeMicrobatch(BaseMicrobatchTestParseError):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "input_model.sql": valid_input_model_sql,
            "microbatch.sql": invalid_event_time_microbatch_model_sql,
        }


class TestMissingBeginMicrobatch(BaseMicrobatchTestParseError):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "input_model.sql": valid_input_model_sql,
            "microbatch.sql": missing_begin_microbatch_model_sql,
        }


class TestInvaliBeginTypeMicrobatch(BaseMicrobatchTestParseError):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "input_model.sql": valid_input_model_sql,
            "microbatch.sql": invalid_begin_microbatch_model_sql,
        }


class TestInvaliBegiFormatMicrobatch(BaseMicrobatchTestParseError):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "input_model.sql": valid_input_model_sql,
            "microbatch.sql": valid_microbatch_model_no_config_sql,
            "microbatch.yml": invalid_microbatch_model_config_yml,
        }


class TestMissingBatchSizeMicrobatch(BaseMicrobatchTestParseError):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "input_model.sql": valid_input_model_sql,
            "microbatch.sql": missing_batch_size_microbatch_model_sql,
        }


class TestInvalidBatchSizeMicrobatch(BaseMicrobatchTestParseError):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "input_model.sql": valid_input_model_sql,
            "microbatch.sql": invalid_batch_size_microbatch_model_sql,
        }


class TestInvalidInputEventTimeMicrobatch(BaseMicrobatchTestParseError):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "input_model.sql": invalid_event_time_input_model_sql,
            "microbatch.sql": valid_microbatch_model_sql,
        }


class TestValidBeginMicrobatch(BaseMicrobatchTestNoError):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "input_model.sql": valid_input_model_sql,
            "microbatch.sql": valid_microbatch_model_no_config_sql,
            "schema.yml": valid_microbatch_model_config_yml,
        }
