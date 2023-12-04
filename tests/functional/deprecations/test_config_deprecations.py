import pytest

from dbt import deprecations
import dbt.exceptions
from dbt.tests.util import run_dbt
from dbt.tests.fixtures.project import write_project_files

from tests.functional.deprecations.fixtures import (
    models_trivial__model_sql,
    old_tests_yaml,
    local_dependency__dbt_project_yml,
    local_dependency__schema_yml,
    local_dependency__seed_csv,
    data_tests_yaml,
    seed_csv,
    sources_old_tests_yaml,
)


# test deprecation messages
class TestTestsConfigDeprecation:
    @pytest.fixture(scope="class")
    def models(self):
        return {"model.sql": models_trivial__model_sql}

    @pytest.fixture(scope="class")
    def project_config_update(self, unique_schema):
        return {"tests": {"enabled": "true"}}

    def test_tests_config(self, project):
        deprecations.reset_deprecations()
        assert deprecations.active_deprecations == set()
        run_dbt(["parse"])
        expected = {"project-test-config"}
        assert expected == deprecations.active_deprecations

    def test_tests_config_fail(self, project):
        deprecations.reset_deprecations()
        assert deprecations.active_deprecations == set()
        with pytest.raises(dbt.exceptions.CompilationError) as exc:
            run_dbt(["--warn-error", "--no-partial-parse", "parse"])
        exc_str = " ".join(str(exc.value).split())  # flatten all whitespace
        expected_msg = "The `tests` config has been renamed to `data-tests`"
        assert expected_msg in exc_str


class TestSchemaTestDeprecation:
    @pytest.fixture(scope="class")
    def models(self):
        return {"model.sql": models_trivial__model_sql, "schema.yml": old_tests_yaml}

    def test_tests_config(self, project):
        deprecations.reset_deprecations()
        assert deprecations.active_deprecations == set()
        run_dbt(["parse"])
        expected = {"project-test-config"}
        assert expected == deprecations.active_deprecations

    def test_schema_tests_fail(self, project):
        deprecations.reset_deprecations()
        assert deprecations.active_deprecations == set()
        with pytest.raises(dbt.exceptions.CompilationError) as exc:
            run_dbt(["--warn-error", "--no-partial-parse", "parse"])
        exc_str = " ".join(str(exc.value).split())  # flatten all whitespace
        expected_msg = "The `tests` config has been renamed to `data_tests`"
        assert expected_msg in exc_str


class TestSourceSchemaTestDeprecation:
    @pytest.fixture(scope="class")
    def models(self):
        return {"schema.yml": sources_old_tests_yaml}

    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "seed.csv": seed_csv,
        }

    def test_source_tests_config(self, project):
        deprecations.reset_deprecations()
        assert deprecations.active_deprecations == set()
        run_dbt(["seed"])
        run_dbt(["parse"])
        expected = {"project-test-config"}
        assert expected == deprecations.active_deprecations

    def test_schema_tests(self, project):
        run_dbt(["seed"])
        results = run_dbt(["test"])
        assert len(results) == 1


# test a local dependency can have tests while the rest of the project uses data_tests
class TestTestConfigInDependency:
    @pytest.fixture(scope="class", autouse=True)
    def setUp(self, project_root):
        local_dependency_files = {
            "dbt_project.yml": local_dependency__dbt_project_yml,
            "models": {
                "schema.yml": local_dependency__schema_yml,
            },
            "seeds": {"seed.csv": local_dependency__seed_csv},
        }
        write_project_files(project_root, "local_dependency", local_dependency_files)

    @pytest.fixture(scope="class")
    def packages(self):
        return {"packages": [{"local": "local_dependency"}]}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "model.sql": models_trivial__model_sql,
            "schema.yml": data_tests_yaml,
        }

    def test_test_dep(self, project):
        run_dbt(["deps"])
        run_dbt(["seed"])
        run_dbt(["run"])
        results = run_dbt(["test"])
        # 1 data_test in the dep and 1 in the project
        assert len(results) == 2


# test selecting resource-type data_tests and return tests
class TestConfigResourceTypeDataTest:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "model.sql": models_trivial__model_sql,
            "schema.yml": old_tests_yaml,  # has tests key
        }

    def expect_select(self):
        results = run_dbt(["ls", "--resource-type", "data_test"])
        assert len(results) == 1
        assert "data_test.test.not_null_model_id" in results


# test selecting resource-type tests and return data_tests
class TestConfigResourceTypeTest:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "model.sql": models_trivial__model_sql,
            "schema.yml": data_tests_yaml,  # has data_tests key
        }

    def expect_select(self):
        results = run_dbt(["ls", "--resource-type", "test"])
        assert len(results) == 1
        assert "data_test.test.not_null_model_id" in results


# test selecting resource-type tests and return tests as data_tests
class TestConfigAllTest:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "model.sql": models_trivial__model_sql,
            "schema.yml": old_tests_yaml,  # has tests key
        }

    def expect_select(self):
        results = run_dbt(["ls", "--resource-type", "test"])
        assert len(results) == 1
        assert "data_test.test.not_null_model_id" in results
