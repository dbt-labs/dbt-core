import pytest

from dbt.tests.util import run_dbt

_MODELS_COLUMN_QUOTING_DEFAULT = """
{% set col_a = '"col_A"' %}
{% set col_b = '"col_B"' %}

{{config(
    materialized = 'incremental',
    unique_key = col_a,
    incremental_strategy = var('strategy')
    )}}

select
{{ col_a }}, {{ col_b }}
from {{ref('seed')}}
"""

_MODELS_COLUMN_QUOTING_NO_QUOTING = """
{% set col_a = '"col_a"' %}
{% set col_b = '"col_b"' %}

{{config(
    materialized = 'incremental',
    unique_key = col_a,
    incremental_strategy = var('strategy')
    )}}

select
{{ col_a }}, {{ col_b }}
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
        return {"model.sql": _MODELS_COLUMN_QUOTING_DEFAULT}

    @pytest.fixture(scope="class")
    def seeds(self):
        return {"seed.csv": _SEEDS_BASIC_SEED}

    @pytest.fixture(scope="function")
    def run_column_quotes(self, project, request):
        def fixt():
            strategy_vars = '{{"strategy": "{}"}}'.format(request.param)
            results = run_dbt(["seed", "--vars", strategy_vars])
            assert len(results) == 1
            results = run_dbt(["run", "--vars", strategy_vars])
            assert len(results) == 1
            results = run_dbt(["run", "--vars", strategy_vars])
            assert len(results) == 1

        return fixt


class TestColumnQuotingDefault(BaseColumnQuotingTest):
    @pytest.mark.parametrize("run_column_quotes", ["delete+insert"], indirect=True)
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

    @pytest.mark.parametrize("run_column_quotes", ["delete+insert"], indirect=True)
    def test_column_quotes(self, run_column_quotes):
        run_column_quotes()


class TestColumnQuotingDisabled(BaseColumnQuotingTest):
    @pytest.fixture(scope="class")
    def models(self):
        return {"model.sql": _MODELS_COLUMN_QUOTING_NO_QUOTING}

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "seeds": {
                "quote_columns": False,
            },
        }

    @pytest.mark.parametrize("run_column_quotes", ["delete+insert"], indirect=True)
    def test_column_quotes(self, run_column_quotes):
        run_column_quotes()
