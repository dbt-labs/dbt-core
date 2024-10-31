import os

import pytest
import yaml

from dbt.tests.util import run_dbt, run_dbt_and_capture
from tests.functional.sources.fixtures import (
    models_descendant_model_sql,
    models_ephemeral_model_sql,
    models_multi_source_model_sql,
    models_nonsource_descendant_sql,
    models_schema_yml,
    models_view_model_sql,
    seeds_expected_multi_source_csv,
    seeds_other_source_table_csv,
    seeds_other_table_csv,
    seeds_source_csv,
)


class BaseSourcesTest:
    @pytest.fixture(scope="class", autouse=True)
    def setEnvVars(self):
        os.environ["DBT_TEST_SCHEMA_NAME_VARIABLE"] = "test_run_schema"

        yield

        del os.environ["DBT_TEST_SCHEMA_NAME_VARIABLE"]

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "schema.yml": models_schema_yml,
            "view_model.sql": models_view_model_sql,
            "ephemeral_model.sql": models_ephemeral_model_sql,
            "descendant_model.sql": models_descendant_model_sql,
            "multi_source_model.sql": models_multi_source_model_sql,
            "nonsource_descendant.sql": models_nonsource_descendant_sql,
        }

    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "source.csv": seeds_source_csv,
            "other_table.csv": seeds_other_table_csv,
            "expected_multi_source.csv": seeds_expected_multi_source_csv,
            "other_source_table.csv": seeds_other_source_table_csv,
        }

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "config-version": 2,
            "seed-paths": ["seeds"],
            "quoting": {"database": True, "schema": True, "identifier": True},
            "seeds": {
                "quote_columns": True,
            },
        }

    def _extend_cmd_with_vars(self, project, cmd):
        vars_dict = {
            "test_run_schema": project.test_schema,
            "test_loaded_at": project.adapter.quote("updated_at"),
        }
        cmd.extend(["--vars", yaml.safe_dump(vars_dict)])

    def run_dbt_with_vars(self, project, cmd, *args, **kwargs):
        self._extend_cmd_with_vars(project, cmd)
        return run_dbt(cmd, *args, **kwargs)

    def run_dbt_and_capture_with_vars(self, project, cmd, *args, **kwargs):
        self._extend_cmd_with_vars(project, cmd)
        return run_dbt_and_capture(cmd, *args, **kwargs)
