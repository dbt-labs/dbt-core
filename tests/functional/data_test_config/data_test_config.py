import os

import pytest

from dbt.tests.fixtures.project import write_project_files
from dbt.tests.util import run_dbt
from tests.functional.data_test_config.fixtures import (
    custom_config_yml,
    empty_configuration_yml,
    mixed_config_yml,
    same_key_error_yml,
    seed_csv,
    table_sql,
)


class BaseDataTestsConfig:
    @pytest.fixture(scope="class")
    def seeds(self):
        return {"seed.csv": seed_csv}

    @pytest.fixture(scope="class")
    def models(self):
        return {"table.sql": table_sql}

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "config-version": 2,
        }

    @pytest.fixture(scope="class", autouse=True)
    def setUp(self, project, project_root, seeds, models):
        write_project_files(project_root, "seeds", seeds)
        write_project_files(project_root, "models", models)
        project.run_sql_file(os.path.join(project_root, "seeds", "seed.csv"))


class TestEmptyDataTestConfig(BaseDataTestsConfig):
    @pytest.fixture(scope="class")
    def models(self):
        return {"table.sql": table_sql, "empty_config.yml": empty_configuration_yml}

    def test_empty_configuration(self, project):
        """Test with empty configuration"""
        results = run_dbt(["test", "--models", "empty_config"], expect_pass=False)
        assert len(results) == 1
        assert results[0].status == "fail"


class TestCustomDataTestConfig(BaseDataTestsConfig):
    @pytest.fixture(scope="class")
    def models(self):
        return {"table.sql": table_sql, "custom_config.yml": custom_config_yml}

    def test_custom_config(self, project):
        """Test with custom configuration"""
        results = run_dbt(["test", "--models", "custom_config"], expect_pass=False)
        assert len(results) == 1
        assert "custom_config_key" in results[0].message


class TestMixedDataTestConfig(BaseDataTestsConfig):
    @pytest.fixture(scope="class")
    def models(self):
        return {"table.sql": table_sql, "mixed_config.yml": mixed_config_yml}

    def test_mixed_config(self, project):
        """Test with mixed configuration"""
        results = run_dbt(["test", "--models", "mixed_config"], expect_pass=False)
        assert len(results) == 1
        assert results[0].status == "fail"
        assert "severity" in results[0].message
        assert "custom_config_key" in results[0].message


class TestSameKeyErrorDataTestConfig(BaseDataTestsConfig):
    @pytest.fixture(scope="class")
    def models(self):
        return {"table.sql": table_sql, "same_key_error.yml": same_key_error_yml}

    def test_same_key_error(self, project):
        """Test with conflicting configuration keys"""
        with pytest.raises(Exception):
            run_dbt(["test", "--models", "same_key_error"], expect_pass=False)
