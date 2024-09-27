import os

import pytest
from functional.data_test_patch.fixtures import tests__malformed_schema_yml

from dbt.tests.util import get_artifact, run_dbt, run_dbt_and_capture
from tests.functional.data_test_patch.fixtures import (
    tests__doc_block_md,
    tests__invalid_name_schema_yml,
    tests__my_singular_test_sql,
    tests__schema_yml,
)


class TestPatchSingularTest:
    @pytest.fixture(scope="class")
    def tests(self):
        return {
            "my_singular_test.sql": tests__my_singular_test_sql,
            "schema.yml": tests__schema_yml,
            "doc_block.md": tests__doc_block_md,
        }

    def test_compile(self, project):
        run_dbt(["compile"])
        manifest = get_artifact(project.project_root, "target", "manifest.json")
        assert len(manifest["nodes"]) == 1

        my_singular_test_node = manifest["nodes"]["test.test.my_singular_test"]
        assert my_singular_test_node["description"] == "Some docs from a doc block"
        assert my_singular_test_node["config"]["error_if"] == ">10"
        assert my_singular_test_node["config"]["meta"] == {"some_key": "some_val"}


class TestPatchSingularTestInvalidName:
    @pytest.fixture(scope="class")
    def tests(self):
        return {
            "my_singular_test.sql": tests__my_singular_test_sql,
            "schema_with_invalid_name.yml": tests__invalid_name_schema_yml,
        }

    def test_compile(self, project):
        _, log_output = run_dbt_and_capture(["compile"])

        file_path = (
            "tests\\schema_with_invalid_name.yml"
            if os.name == "nt"
            else "tests/schema_with_invalid_name.yml"
        )
        assert (
            f"Did not find matching node for patch with name 'my_double_test' in the 'data_tests' section of file '{file_path}'"
            in log_output
        )


class TestPatchSingularTestMalformedYaml:
    @pytest.fixture(scope="class")
    def tests(self):
        return {
            "my_singular_test.sql": tests__my_singular_test_sql,
            "schema.yml": tests__malformed_schema_yml,
        }

    def test_compile(self, project):
        _, log_output = run_dbt_and_capture(["compile"])
        file_path = "tests\\schema.yml" if os.name == "nt" else "tests/schema.yml"
        assert f"Unable to parse 'data_tests' section of file '{file_path}'" in log_output
        assert "Entry did not contain a name" in log_output
