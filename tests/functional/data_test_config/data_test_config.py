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


@pytest.fixture(scope="class", autouse=True)
def setUp(project, project_root):
    seed_file_path = os.path.join(project.test_data_dir, "seed.csv")
    with open(seed_file_path, "w") as f:
        f.write(seed_csv)
    project.run_sql_file(seed_file_path)

    models = {
        "empty_configuration.yml": empty_configuration_yml,
        "custom_config.yml": custom_config_yml,
        "mixed_config.yml": mixed_config_yml,
        "same_key_error.yml": same_key_error_yml,
        "table_copy.sql": table_sql,
    }
    write_project_files(project_root, "models", models)


@pytest.fixture(scope="class")
def project_config_update():
    return {
        "config-version": 2,
    }


class TestConfigSchema:
    def test_empty_configuration(self, project):
        """Test with empty configuration"""
        results = run_dbt(["test", "--models", "empty_configuration"], expect_pass=False)
        assert len(results) == 1
        assert results[0].status == "fail"

    def test_custom_config(self, project):
        """Test with custom configuration"""
        results = run_dbt(["test", "--models", "custom_config"], expect_pass=False)
        assert len(results) == 1
        assert results[0].status == "fail"

    def test_mixed_config(self, project):
        """Test with mixed configuration"""
        results = run_dbt(["test", "--models", "mixed_config"], expect_pass=False)
        assert len(results) == 1
        assert results[0].status == "fail"
        assert "severity" in results[0].message

    def test_same_key_error(self, project):
        """Test with conflicting configuration keys"""
        with pytest.raises(Exception):
            run_dbt(["test", "--models", "same_key_error"], expect_pass=False)


# class BaseDataTestsConfig:
#     @pytest.fixture(scope="class")
#     def seeds(self):
#         return { "seed.csv": seed_csv}

#     @pytest.fixture(scope="class")
#     def models(self):
#         return {"table.sql": table_sql}


#     def test_data_test_config_setup(self, project):
#        run_dbt(["seed"])
