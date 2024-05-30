import pytest

from dbt.exceptions import CompilationError
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
    def setUp(self, project):
        run_dbt(["seed"])


class TestEmptyDataTestConfig(BaseDataTestsConfig):
    @pytest.fixture(scope="class")
    def models(self):
        return {"table.sql": table_sql, "empty_config.yml": empty_configuration_yml}

    def test_empty_configuration(self, project):
        run_dbt(["run"])
        """Test with empty configuration"""
        run_dbt(["test", "--models", "empty_config"])


class TestCustomDataTestConfig(BaseDataTestsConfig):
    @pytest.fixture(scope="class")
    def models(self):
        return {"table.sql": table_sql, "custom_config.yml": custom_config_yml}

    def test_custom_config(self, project):
        run_dbt(["run"])
        """Test with custom configuration"""
        run_dbt(["test", "--models", "custom_config"])


class TestMixedDataTestConfig(BaseDataTestsConfig):
    @pytest.fixture(scope="class")
    def models(self):
        return {"table.sql": table_sql, "mixed_config.yml": mixed_config_yml}

    def test_mixed_config(self, project):
        run_dbt(["run"])
        """Test with mixed configuration"""
        run_dbt(["test", "--models", "mixed_config"])


class TestSameKeyErrorDataTestConfig(BaseDataTestsConfig):
    @pytest.fixture(scope="class")
    def models(self):
        return {"table.sql": table_sql, "same_key_error.yml": same_key_error_yml}

    def test_same_key_error(self, project):
        run_dbt(["run"])
        """Test with conflicting configuration keys"""
        with pytest.raises(CompilationError) as e:
            run_dbt(["test", "--models", "same_key_error"], expect_pass=False)
        assert "cannot have the same key at the top-level and in config" in str(e.value)
