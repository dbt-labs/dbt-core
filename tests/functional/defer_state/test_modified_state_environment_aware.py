import os

import pytest

from dbt.tests.util import run_dbt
from tests.functional.defer_state.fixtures import (  # model_with_env_var_in_config_sql,;; schema_source_with_env_var_as_property_yml,
    model_with_no_in_config_sql,
    schema_model_with_env_var_in_config_yml,
)
from tests.functional.defer_state.test_modified_state import BaseModifiedState


class BaseNodeWithEnvVarConfig(BaseModifiedState):
    @pytest.fixture(scope="class")
    def seeds(self):
        return {}

    @pytest.fixture(scope="class", autouse=True)
    def setup(self):
        os.environ["DBT_TEST_STATE_MODIFIED"] = "table"
        yield
        del os.environ["DBT_TEST_STATE_MODIFIED"]

    def test_change_env_var(self, project):
        # Generate ./state without changing environment variable value
        run_dbt(["run"])
        self.copy_state()

        # Assert no false positive
        results = run_dbt(["list", "-s", "state:modified", "--state", "./state"])
        assert len(results) == 0

        # Change environment variable and assert no
        # Environment variables do not have an effect on state:modified
        os.environ["DBT_TEST_STATE_MODIFIED"] = "view"
        results = run_dbt(["list", "-s", "state:modified", "--state", "./state"])
        assert len(results) == 0


# class TestModelNodeWithEnvVarConfigInSqlFile(BaseNodeWithEnvVarConfig):
#     @pytest.fixture(scope="class")
#     def models(self):
#         return {
#             "model.sql": model_with_env_var_in_config_sql,
#         }


class TestModelNodeWithEnvVarConfigInSchemaYml(BaseNodeWithEnvVarConfig):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "model.sql": model_with_no_in_config_sql,
            "schema.yml": schema_model_with_env_var_in_config_yml,
        }


class TestModelNodeWithEnvVarConfigInProjectYml(BaseNodeWithEnvVarConfig):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "model.sql": model_with_no_in_config_sql,
        }

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "models": {
                "test": {
                    "+materialized": "{{ env_var('DBT_TEST_STATE_MODIFIED') }}",
                }
            }
        }


class TestModelNodeWithEnvVarConfigInProjectYmlAndSchemaYml(BaseNodeWithEnvVarConfig):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "model.sql": model_with_no_in_config_sql,
            "schema.yml": schema_model_with_env_var_in_config_yml,
        }

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "models": {
                "test": {
                    "+materialized": "{{ env_var('DBT_TEST_STATE_MODIFIED') }}",
                }
            }
        }


# class TestModelNodeWithEnvVarConfigInSqlAndSchemaYml(BaseNodeWithEnvVarConfig):
#     @pytest.fixture(scope="class")
#     def models(self):
#         return {
#             "model.sql": model_with_env_var_in_config_sql,
#             "schema.yml": schema_model_with_env_var_in_config_yml
#         }


# class TestSourceNodeWithEnvVarConfigInSchema(BaseNodeWithEnvVarConfig):
#     @pytest.fixture(scope="class")
#     def models(self):
#         return {
#             "schema.yml": schema_source_with_env_var_as_property_yml,
#             # "model.sql": "select * from {{ source('jaffle_shop', 'customers') }}"
#         }
