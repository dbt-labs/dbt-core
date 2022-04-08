import os
import pytest
import yaml

from dbt.tests.util import run_dbt
from tests.functional.sources.fixtures import (
    models__schema_yml,
    models__view_model_sql,
    models__ephemeral_model_sql,
    models__descendant_model_sql,
    models__multi_source_model_sql,
    models__nonsource_descendant_sql,
    seeds__source_csv,
    seeds__other_table_csv,
    seeds__expected_multi_source_csv,
    seeds__other_source_table_csv,
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
            "schema.yml": models__schema_yml,
            "view_model.sql": models__view_model_sql,
            "ephemeral_model.sql": models__ephemeral_model_sql,
            "descendant_model.sql": models__descendant_model_sql,
            "multi_source_model.sql": models__multi_source_model_sql,
            "nonsource_descendant.sql": models__nonsource_descendant_sql,
        }

    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "source.csv": seeds__source_csv,
            "other_table.csv": seeds__other_table_csv,
            "expected_multi_source.csv": seeds__expected_multi_source_csv,
            "other_source_table.csv": seeds__other_source_table_csv,
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

    def run_dbt_with_vars(self, project, cmd, *args, **kwargs):
        vars_dict = {
            "test_run_schema": project.test_schema,
            "test_loaded_at": project.adapter.quote("updated_at"),
        }
        cmd.extend(["--vars", yaml.safe_dump(vars_dict)])
        return run_dbt(cmd, *args, **kwargs)
