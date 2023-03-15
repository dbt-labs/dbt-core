import pytest

from dbt.tests.util import run_dbt

_MODELS__COLUMN_QUOTING_DEFAULT = """
{% set col_a = '"col_A"' %}
{% set col_b = '"col_B"' %}

{{
  config(
    materialized = 'incremental',
    unique_key = col_a,
  )
}}

select
  {{ col_a }},
  {{ col_b }}
from {{ref('seed')}}
"""

_MODELS__COLUMN_QUOTING_NO_QUOTING = """
{% set col_a = '"col_a"' %}
{% set col_b = '"col_b"' %}

{{
  config(
    materialized = 'incremental',
    unique_key = col_a,
  )
}}

select
  {{ col_a }},
  {{ col_b }}
from {{ref('seed')}}
"""

_SEEDS_BASIC_SEED = """col_A,col_B
1,2
3,4
5,6
"""


class BaseColumnQuotingTest:
    @pytest.fixture(scope="class")
    def models(self):
        return {"model.sql": _MODELS__COLUMN_QUOTING_DEFAULT}

    @pytest.fixture(scope="class")
    def seeds(self):
        return {"seed.csv": _SEEDS_BASIC_SEED}

    @pytest.fixture(scope="function")
    def run_column_quotes(self, project):
        def fixt():
            results = run_dbt(["seed"])
            assert len(results) == 1
            results = run_dbt(["run"])
            assert len(results) == 1
            results = run_dbt(["run"])
            assert len(results) == 1

        return fixt


class TestColumnQuotingDefault(BaseColumnQuotingTest):
    def test_column_quotes(self, run_column_quotes):
        run_column_quotes()


class TestColumnQuotingEnabled(BaseColumnQuotingTest):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "seeds": {
                "quote_columns": True,
            },
        }

    def test_column_quotes(self, run_column_quotes):
        run_column_quotes()


class TestColumnQuotingDisabled(BaseColumnQuotingTest):
    @pytest.fixture(scope="class")
    def models(self):
        return {"model.sql": _MODELS__COLUMN_QUOTING_NO_QUOTING}

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "seeds": {
                "quote_columns": False,
            },
        }

    def test_column_quotes(self, run_column_quotes):
        run_column_quotes()
