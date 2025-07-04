import os
from collections import defaultdict
from typing import Dict

import pytest
from pytest_mock import MockerFixture

from dbt import deprecations
from dbt.exceptions import CompilationError, ProjectContractError, YamlParseDictError
from dbt.tests.fixtures.project import write_project_files
from dbt.tests.util import run_dbt, update_config_file
from tests.functional.deprecations.fixtures import (
    data_tests_yaml,
    local_dependency__dbt_project_yml,
    local_dependency__schema_yml,
    local_dependency__seed_csv,
    macros__custom_test_sql,
    models_trivial__model_sql,
    old_tests_yml,
    seed_csv,
    sources_old_tests_yaml,
    test_type_mixed_yaml,
)


# TODO: This is now defined 4 times throughout the test suite. We should move it to a utils file.
def normalize(path):
    """On windows, neither is enough on its own:

    >>> normcase('C:\\documents/ALL CAPS/subdir\\..')
    'c:\\documents\\all caps\\subdir\\..'
    >>> normpath('C:\\documents/ALL CAPS/subdir\\..')
    'C:\\documents\\ALL CAPS'
    >>> normpath(normcase('C:\\documents/ALL CAPS/subdir\\..'))
    'c:\\documents\\all caps'
    """
    return os.path.normcase(os.path.normpath(path))


# test deprecation messages
class TestTestsConfigDeprecation:
    @pytest.fixture(scope="class")
    def models(self):
        return {"model.sql": models_trivial__model_sql}

    @pytest.fixture(scope="class")
    def project_config_update(self, unique_schema):
        return {"tests": {"enabled": "true"}}

    def test_project_tests_config(self, project):
        deprecations.reset_deprecations()
        assert deprecations.active_deprecations == defaultdict(int)
        run_dbt(["parse"])
        assert deprecations.active_deprecations == defaultdict(int)

    def test_project_tests_config_fail(self, project):
        deprecations.reset_deprecations()
        assert deprecations.active_deprecations == defaultdict(int)
        with pytest.raises(CompilationError) as exc:
            run_dbt(["--warn-error", "--no-partial-parse", "parse"])
        exc_str = " ".join(str(exc.value).split())  # flatten all whitespace
        expected_msg = "Configuration paths exist in your dbt_project.yml file which do not apply to any resources. There are 1 unused configuration paths: - data_tests"
        assert expected_msg in exc_str


class TestSchemaTestDeprecation:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "model.sql": models_trivial__model_sql,
            "versioned_model.sql": models_trivial__model_sql,
            "schema.yml": old_tests_yml,
        }

    @pytest.fixture(scope="class")
    def macros(self):
        return {"custom.sql": macros__custom_test_sql}

    def test_generic_tests_config(self, project):
        deprecations.reset_deprecations()
        assert deprecations.active_deprecations == defaultdict(int)
        run_dbt(["parse"])
        assert deprecations.active_deprecations == defaultdict(int)

    def test_generic_tests_fail(self, project):
        deprecations.reset_deprecations()
        assert deprecations.active_deprecations == defaultdict(int)
        run_dbt(["--warn-error", "--no-partial-parse", "parse"])

    def test_generic_data_test_parsing(self, project):
        results = run_dbt(["list", "--resource-type", "test"])
        assert len(results) == 4


class TestSourceSchemaTestDeprecation:
    @pytest.fixture(scope="class")
    def models(self):
        return {"schema.yml": sources_old_tests_yaml}

    @pytest.fixture(scope="class")
    def macros(self):
        return {"custom.sql": macros__custom_test_sql}

    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "seed.csv": seed_csv,
        }

    def test_source_tests_config(self, project):
        deprecations.reset_deprecations()
        assert deprecations.active_deprecations == defaultdict(int)
        run_dbt(["parse"])
        assert deprecations.active_deprecations == defaultdict(int)

    def test_generic_data_tests(self, project):
        run_dbt(["seed"])
        results = run_dbt(["test"])
        assert len(results) == 2


# test for failure with test and data_tests in the same file
class TestBothSchemaTestDeprecation:
    @pytest.fixture(scope="class")
    def models(self):
        return {"model.sql": models_trivial__model_sql, "schema.yml": test_type_mixed_yaml}

    def test_schema(self, project):
        expected_msg = "Invalid test config: cannot have both 'tests' and 'data_tests' defined"
        with pytest.raises(YamlParseDictError) as excinfo:
            run_dbt(["parse"])
        assert expected_msg in str(excinfo.value)


# test for failure with  test and data_tests in the same dbt_project.yml
class TestBothProjectTestDeprecation:
    @pytest.fixture(scope="class")
    def models(self):
        return {"model.sql": models_trivial__model_sql}

    def test_tests_config(self, project):
        config_patch = {"tests": {"+enabled": "true"}, "data_tests": {"+tags": "super"}}
        update_config_file(config_patch, project.project_root, "dbt_project.yml")

        expected_msg = "Invalid project config: cannot have both 'tests' and 'data_tests' defined"
        with pytest.raises(ProjectContractError) as excinfo:
            run_dbt(["parse"])
        assert expected_msg in str(excinfo.value)


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


class TestValidateModelConfigOnlyCalledOncePerModel:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "model.sql": models_trivial__model_sql,
            "versioned_model.sql": models_trivial__model_sql,
            "schema.yml": old_tests_yml,
        }

    @pytest.fixture(scope="class")
    def macros(self):
        return {"custom.sql": macros__custom_test_sql}

    def test_validate_model_config_only_called_once_per_model(
        self, project, models: Dict[str, str], mocker: MockerFixture
    ):
        mocked_vmc = mocker.patch("dbt.parser.base.validate_model_config")
        run_dbt(
            ["parse", "--no-partial-parse"],
        )
        # this is a list of all the files that validate_model_config was called on
        called_on_files = [call.args[1] for call in mocked_vmc.call_args_list]

        # this entire test is only useful if we have validated more than one file
        assert len(called_on_files) > 1

        # Each model sql file should have been validated once
        assert len(called_on_files) == len(set(called_on_files))

        # Only model sql files should have been validated
        sql_model_files = [file for file in models.keys() if file.endswith(".sql")]
        assert len(called_on_files) == len(sql_model_files)
        for file in sql_model_files:
            assert normalize(f"models/{file}") in called_on_files
