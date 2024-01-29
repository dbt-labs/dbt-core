import pytest
from dbt.tests.util import run_dbt
import json
import os

from fixtures import (  # noqa: F401
    my_model_vars_sql,
    my_model_a_sql,
    my_model_b_sql,
    test_my_model_yml,
    datetime_test,
)


class TestUnitTestList:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model.sql": my_model_vars_sql,
            "my_model_a.sql": my_model_a_sql,
            "my_model_b.sql": my_model_b_sql,
            "test_my_model.yml": test_my_model_yml + datetime_test,
        }

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"vars": {"my_test": "my_test_var"}}

    def test_unit_test_list(self, project):
        # make sure things are working
        results = run_dbt(["run"])
        assert len(results) == 3
        results = run_dbt(["test"], expect_pass=False)
        assert len(results) == 5

        results = run_dbt(["list"])
        expected = [
            "test.my_model",
            "test.my_model_a",
            "test.my_model_b",
            "unit_test:test.test_my_model",
            "unit_test:test.test_my_model_datetime",
            "unit_test:test.test_my_model_empty",
            "unit_test:test.test_my_model_overrides",
            "unit_test:test.test_my_model_string_concat",
        ]
        assert sorted(results) == sorted(expected)

        results = run_dbt(["list", "--select", "test_type:unit"])
        assert len(results) == 5

        # Check json result
        results = run_dbt(["list", "--select", "test_type:unit", "--output", "json"])
        expected_test_my_model = {
            "name": "test_my_model",
            "resource_type": "unit_test",
            "package_name": "test",
            "original_file_path": os.path.join("models", "test_my_model.yml"),
            "unique_id": "unit_test.test.my_model.test_my_model",
            "depends_on": {"macros": [], "nodes": ["model.test.my_model"]},
            "config": {"tags": [], "meta": {}},
        }
        for result in results:
            json_result = json.loads(result)
            if "name" in json_result and json_result["name"] == "test_my_model":
                assert json_result == expected_test_my_model

        results = run_dbt(
            [
                "list",
                "--select",
                "test_type:unit",
                "--output",
                "json",
                "--output-keys",
                "unique_id",
                "model",
            ]
        )
        for result in results:
            json_result = json.loads(result)
            assert json_result["model"] == "my_model"
