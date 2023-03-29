import pytest
from dbt.tests.util import run_dbt, check_relations_equal

from dbt.tests.adapter.simple_copy.fixtures import (
    _PROPERTIES__SCHEMA_YML,
    _SEEDS__SEED_INITIAL,
    _MODELS__ADVANCED_INCREMENTAL,
    _MODELS__COMPOUND_SORT,
    _MODELS__DISABLED,
    _MODELS__EMPTY,
    _MODELS_GET_AND_REF_UPPERCASE,
    _MODELS__INCREMENTAL,
    _MODELS__INTERLEAVED_SORT,
    _MODELS__MATERIALIZED,
    _MODELS__VIEW_MODEL,
)


class TestSimpleCopyUppercase:
    @pytest.fixture(scope="class")
    def dbt_profile_target(self):
        return {
            "type": "postgres",
            "threads": 4,
            "host": "localhost",
            "port": 5432,
            "user": "root",
            "pass": "password",
            "dbname": "dbtMixedCase",
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "ADVANCED_INCREMENTAL.sql": _MODELS__ADVANCED_INCREMENTAL,
            "COMPOUND_SORT.sql": _MODELS__COMPOUND_SORT,
            "DISABLED.sql": _MODELS__DISABLED,
            "EMPTY.sql": _MODELS__EMPTY,
            "GET_AND_REF.sql": _MODELS_GET_AND_REF_UPPERCASE,
            "INCREMENTAL.sql": _MODELS__INCREMENTAL,
            "INTERLEAVED_SORT.sql": _MODELS__INTERLEAVED_SORT,
            "MATERIALIZED.sql": _MODELS__MATERIALIZED,
            "VIEW_MODEL.sql": _MODELS__VIEW_MODEL,
        }

    @pytest.fixture(scope="class")
    def properties(self):
        return {
            "schema.yml": _PROPERTIES__SCHEMA_YML,
        }

    @pytest.fixture(scope="class")
    def seeds(self):
        return {"seed.csv": _SEEDS__SEED_INITIAL}

    def test_simple_copy_uppercase(self, project):

        # Load the seed file and check that it worked
        results = run_dbt(["seed"])
        assert len(results) == 1

        # Run the project and ensure that all the models loaded
        results = run_dbt()
        assert len(results) == 7

        check_relations_equal(
            project.adapter, ["seed", "VIEW_MODEL", "INCREMENTAL", "MATERIALIZED", "GET_AND_REF"]
        )
