import pytest
from pathlib import Path
import os
from dbt.tests.util import (
    run_dbt,
    read_file,
    check_relations_equal,
    check_table_does_exist,
    check_table_does_not_exist,
)
from tests.functional.simple_seed.fixtures import models__downstream_from_seed_actual

# from `test/integration/test_simple_seed`, test_postgres_simple_seed


class SeedTestBase(object):
    @pytest.fixture(scope="class", autouse=True)
    def setUp(self, project):
        """Create table for ensuring seeds and models used in tests build correctly"""
        project.run_sql_file(project.test_data_dir / Path("seed_expected.sql"))

    @pytest.fixture(scope="class")
    def seeds(self):
        dir_path = os.path.dirname(os.path.realpath(__file__))
        seed_actual_csv = read_file(dir_path, "seeds", "seed_actual.csv")
        return {
            # "model.sql": model,
            "seed_actual.csv": seed_actual_csv
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "models__downstream_from_seed_actual.sql": models__downstream_from_seed_actual,
        }

    def _build_relations_for_test(self, project):
        """The testing environment needs seeds and models to interact with"""
        seed_result = run_dbt(["seed"])
        assert len(seed_result) == 1
        check_relations_equal(project.adapter, ["seed_expected", "seed_actual"])

        run_result = run_dbt()
        assert len(run_result) == 1
        check_relations_equal(
            project.adapter, ["models__downstream_from_seed_actual", "seed_expected"]
        )

    def _check_relation_end_state(self, run_result, project, exists: bool):
        assert len(run_result) == 1
        check_relations_equal(project.adapter, ["seed_actual", "seed_expected"])
        if exists:
            check_table_does_exist(project.adapter, "models__downstream_from_seed_actual")
        else:
            check_table_does_not_exist(project.adapter, "models__downstream_from_seed_actual")


class TestBasicSeedTests(SeedTestBase):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "config-version": 2,
            "seed-paths": ["seeds"],
            "seeds": {
                "quote_columns": False,
            },
        }

    def test_postgres_simple_seed(self, project):
        """Build models and observe that run truncates a seed and re-inserts rows"""
        self._build_relations_for_test(project)
        self._check_relation_end_state(run_result=run_dbt(["seed"]), project=project, exists=True)

    def test_postgres_simple_seed_full_refresh_flag(self, project):
        """Drop the seed_actual table and re-create. Verifies correct behavior by the absence of the
        model which depends on seed_actual."""
        self._build_relations_for_test(project)
        self._check_relation_end_state(
            run_result=run_dbt(["seed", "--full-refresh"]), project=project, exists=False
        )


class TestSeedConfigFullRefreshOn(SeedTestBase):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "config-version": 2,
            "seed-paths": ["seeds"],
            "seeds": {"quote_columns": False, "full_refresh": True},
        }

    def test_postgres_simple_seed_full_refresh_config(self, project):
        """config option should drop current model and cascade drop to downstream models"""
        self._build_relations_for_test(project)
        self._check_relation_end_state(run_result=run_dbt(["seed"]), project=project, exists=False)


class TestSeedConfigFullRefreshOff(SeedTestBase):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "config-version": 2,
            "seed-paths": ["seeds"],
            "seeds": {"quote_columns": False, "full_refresh": False},
        }

    def test_postgres_simple_seed_full_refresh_config(self, project):
        """Config options should override full-refresh flag because config is higher priority"""
        self._build_relations_for_test(project)
        self._check_relation_end_state(run_result=run_dbt(["seed"]), project=project, exists=True)
        self._check_relation_end_state(
            run_result=run_dbt(["seed", "--full-refresh"]), project=project, exists=True
        )
