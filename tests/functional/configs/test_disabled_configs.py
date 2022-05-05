import pytest

from dbt.tests.util import run_dbt

from tests.functional.configs.fixtures import BaseConfigProject


class TestDisabledConfigs(BaseConfigProject):
    @pytest.fixture(scope="class")
    def dbt_profile_data(self, unique_schema):
        return {
            "config": {"send_anonymous_usage_stats": False},
            "test": {
                "outputs": {
                    "default": {
                        "type": "postgres",
                        # make sure you can do this and get an int out
                        "threads": "{{ (1 + 3) | as_number }}",
                        "host": "localhost",
                        "port": "{{ (5400 + 32) | as_number }}",
                        "user": "root",
                        "pass": "password",
                        "dbname": "dbt",
                        "schema": unique_schema,
                    },
                    "disabled": {
                        "type": "postgres",
                        # make sure you can do this and get an int out
                        "threads": "{{ (1 + 3) | as_number }}",
                        "host": "localhost",
                        "port": "{{ (5400 + 32) | as_number }}",
                        "user": "root",
                        "pass": "password",
                        "dbname": "dbt",
                        "schema": unique_schema,
                    },
                },
                "target": "default",
            },
        }

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "models": {
                "test": {
                    "enabled": "{{ (target.name == 'default' | as_bool) }}",
                },
            },
            # set the `var` result in schema.yml to be 'seed', so that the
            # `source` call can suceed.
            "vars": {
                "test": {
                    "seed_name": "seed",
                }
            },
            "seeds": {
                "quote_columns": False,
                "test": {
                    "seed": {
                        "enabled": "{{ (target.name == 'default') | as_bool }}",
                    },
                },
            },
            "tests": {
                "test": {
                    "enabled": "{{ (target.name == 'default') | as_bool }}",
                    "severity": "WARN",
                },
            },
        }

    def test_disable_seed_partial_parse(self, project):
        run_dbt(["--partial-parse", "seed", "--target", "disabled"])
        run_dbt(["--partial-parse", "seed", "--target", "disabled"])

    def test_conditional_model(self, project):
        # no seeds/models - enabled should eval to False because of the target
        results = run_dbt(["seed", "--target", "disabled"])
        assert len(results) == 0
        results = run_dbt(["run", "--target", "disabled"])
        assert len(results) == 0
        results = run_dbt(["test", "--target", "disabled"])
        assert len(results) == 0

        # has seeds/models - enabled should eval to True because of the target
        results = run_dbt(["seed"])
        assert len(results) == 1
        results = run_dbt(["run"])
        assert len(results) == 2
        results = run_dbt(["test"])
        assert len(results) == 5
