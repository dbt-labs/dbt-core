import os
import pytest
from dbt.tests.util import run_dbt, copy_file, write_file
from pathlib import Path
from tests.functional.metrics.fixtures import (
    metrics_1_yml,
    metrics_2_yml,
    model_a_sql,
    model_b_sql,
)


class TestMetricDeferral:
    @pytest.fixture(scope="class", autouse=True)
    def setup(self, project):
        # Create "prod" schema
        prod_schema_name = project.test_schema + "_prod"
        project.create_test_schema(schema_name=prod_schema_name)
        # Create "state" directory
        path = Path(project.project_root) / "state"
        Path.mkdir(path)

    @pytest.fixture(scope="class")
    def dbt_profile_data(self, unique_schema):
        return {
            "config": {"send_anonymous_usage_stats": False},
            "test": {
                "outputs": {
                    "default": {
                        "type": "postgres",
                        "threads": 4,
                        "host": "localhost",
                        "port": int(os.getenv("POSTGRES_TEST_PORT", 5432)),
                        "user": os.getenv("POSTGRES_TEST_USER", "root"),
                        "pass": os.getenv("POSTGRES_TEST_PASS", "password"),
                        "dbname": os.getenv("POSTGRES_TEST_DATABASE", "dbt"),
                        "schema": unique_schema,
                    },
                    "prod": {
                        "type": "postgres",
                        "threads": 4,
                        "host": "localhost",
                        "port": int(os.getenv("POSTGRES_TEST_PORT", 5432)),
                        "user": os.getenv("POSTGRES_TEST_USER", "root"),
                        "pass": os.getenv("POSTGRES_TEST_PASS", "password"),
                        "dbname": os.getenv("POSTGRES_TEST_DATABASE", "dbt"),
                        "schema": unique_schema + "_prod",
                    },
                },
                "target": "default",
            },
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "model_a.sql": model_a_sql,
            "model_b.sql": model_b_sql,
            "metrics.yml": metrics_1_yml,
        }

    @pytest.mark.skip("TODO")
    def test_metric_deferral(self, project):
        results = run_dbt(["run", "--target", "prod"])
        assert len(results) == 2

        # copy manifest.json to "state" directory
        target_path = os.path.join(project.project_root, "target")
        copy_file(target_path, "manifest.json", project.project_root, ["state", "manifest.json"])

        # Change metrics file
        write_file(metrics_2_yml, project.project_root, "models", "metrics.yml")

        # Confirm that some_metric + model_b are both selected, and model_a is not selected
        results = run_dbt(["ls", "-s", "state:modified+", "--state", "state/", "--target", "prod"])
        assert results == ["metric:test.some_metric", "test.model_b"]

        # Run in default schema
        results = run_dbt(
            ["run", "-s", "state:modified+", "--state", "state/", "--defer", "--target", "default"]
        )
        assert len(results) == 1
