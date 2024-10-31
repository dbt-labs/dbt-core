import pytest

from dbt.tests.util import get_manifest, run_dbt
from tests.functional.primary_keys.fixtures import (
    simple_model_constraints,
    simple_model_disabled_unique_test,
    simple_model_sql,
    simple_model_two_versions_both_configured,
    simple_model_two_versions_exclude_col,
    simple_model_unique_combo_of_columns,
    simple_model_unique_not_null_tests,
    simple_model_unique_test,
)


class TestSimpleModelNoYml:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "simple_model.sql": simple_model_sql,
        }

    def test_simple_model_no_yml(self, project):
        run_dbt(["deps"])
        run_dbt(["run"])
        manifest = get_manifest(project.project_root)
        node = manifest.nodes["model.test.simple_model"]
        assert node.primary_key == []


class TestSimpleModelConstraints:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "simple_model.sql": simple_model_sql,
            "schema.yml": simple_model_constraints,
        }

    def test_simple_model_constraints(self, project):
        run_dbt(["deps"])
        run_dbt(["run"])
        manifest = get_manifest(project.project_root)
        node = manifest.nodes["model.test.simple_model"]
        assert node.primary_key == ["id"]


class TestSimpleModelUniqueNotNullTests:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "simple_model.sql": simple_model_sql,
            "schema.yml": simple_model_unique_not_null_tests,
        }

    def test_simple_model_unique_not_null_tests(self, project):
        run_dbt(["deps"])
        run_dbt(["run"])
        manifest = get_manifest(project.project_root)
        node = manifest.nodes["model.test.simple_model"]
        assert node.primary_key == ["id"]


class TestSimpleModelUniqueTests:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "simple_model.sql": simple_model_sql,
            "schema.yml": simple_model_unique_test,
        }

    def test_simple_model_unique_test(self, project):
        run_dbt(["deps"])
        run_dbt(["run"])
        manifest = get_manifest(project.project_root)
        node = manifest.nodes["model.test.simple_model"]
        assert node.primary_key == ["id"]


class TestSimpleModelDisabledUniqueTests:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "simple_model.sql": simple_model_sql,
            "schema.yml": simple_model_disabled_unique_test,
        }

    def test_simple_model_disabled_unique_test(self, project):
        run_dbt(["deps"])
        run_dbt(["run"])
        manifest = get_manifest(project.project_root)
        node = manifest.nodes["model.test.simple_model"]
        assert node.primary_key == ["id"]


class TestVersionedSimpleModel:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "simple_model_v1.sql": simple_model_sql,
            "simple_model_v2.sql": simple_model_sql,
            "schema.yml": simple_model_two_versions_both_configured,
        }

    def test_versioned_simple_model(self, project):
        run_dbt(["deps"])
        run_dbt(["run"])
        manifest = get_manifest(project.project_root)
        node_v1 = manifest.nodes["model.test.simple_model.v1"]
        node_v2 = manifest.nodes["model.test.simple_model.v2"]
        assert node_v1.primary_key == ["id"]
        assert node_v2.primary_key == ["id"]


class TestVersionedSimpleModelExcludeTests:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "simple_model_v1.sql": simple_model_sql,
            "simple_model_v2.sql": simple_model_sql,
            "schema.yml": simple_model_two_versions_exclude_col,
        }

    def test_versioned_simple_model_exclude_col(self, project):
        run_dbt(["deps"])
        run_dbt(["run"])
        manifest = get_manifest(project.project_root)
        node_v1 = manifest.nodes["model.test.simple_model.v1"]
        node_v2 = manifest.nodes["model.test.simple_model.v2"]
        assert node_v1.primary_key == ["id"]
        assert node_v2.primary_key == []


class TestSimpleModelCombinationOfColumns:
    @pytest.fixture(scope="class")
    def packages(self):
        return {
            "packages": [
                {
                    "git": "https://github.com/dbt-labs/dbt-utils.git",
                    "revision": "1.1.0",
                },
            ]
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "simple_model.sql": simple_model_sql,
            "schema.yml": simple_model_unique_combo_of_columns,
        }

    def test_versioned_simple_combo_of_columns(self, project):
        run_dbt(["deps"])
        run_dbt(["run"])
        manifest = get_manifest(project.project_root)
        node = manifest.nodes["model.test.simple_model"]
        assert node.primary_key == ["color", "id"]
