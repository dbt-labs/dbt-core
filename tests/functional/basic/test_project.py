import os
from pathlib import Path

import pytest
import yaml

from dbt.cli.main import dbtRunner
from dbt.exceptions import DbtProjectError, ProjectContractError
from dbt.tests.util import run_dbt, update_config_file, write_config_file

simple_model_sql = """
select true as my_column
"""

simple_model_yml = """
models:
  - name: simple_model
    description: "is sythentic data ok? my column:"
    columns:
      - name: my_column
        description: asked and answered
"""


class TestSchemaYmlVersionMissing:
    @pytest.fixture(scope="class")
    def models(self):
        return {"simple_model.sql": simple_model_sql, "simple_model.yml": simple_model_yml}

    def test_empty_version(self, project):
        run_dbt(["run"], expect_pass=True)


class TestProjectConfigVersionMissing:
    # default dbt_project.yml has config-version: 2
    @pytest.fixture(scope="class")
    def project_config_remove(self):
        return ["config-version"]

    def test_empty_version(self, project):
        run_dbt(["run"], expect_pass=True)


class TestProjectYamlVersionMissing:
    # default dbt_project.yml does not fill version

    def test_empty_version(self, project):
        run_dbt(["run"], expect_pass=True)


class TestProjectYamlVersionValid:
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"version": "1.0.0"}

    def test_valid_version(self, project):
        run_dbt(["run"], expect_pass=True)


class TestProjectYamlVersionInvalid:
    def test_invalid_version(self, project):
        # we need to run it so the project gets set up first, otherwise we hit the semver error in setting up the test project
        run_dbt()
        update_config_file({"version": "invalid"}, "dbt_project.yml")
        with pytest.raises(ProjectContractError) as excinfo:
            run_dbt()
        assert "at path ['version']: 'invalid' is not valid under any of the given schemas" in str(
            excinfo.value
        )


class TestProjectDbtCloudConfig:
    @pytest.fixture(scope="class")
    def models(self):
        return {"simple_model.sql": simple_model_sql, "simple_model.yml": simple_model_yml}

    def test_dbt_cloud(self, project):
        run_dbt(["parse"], expect_pass=True)
        conf = yaml.safe_load(
            Path(os.path.join(project.project_root, "dbt_project.yml")).read_text()
        )
        assert conf == {
            "name": "test",
            "profile": "test",
            "flags": {"send_anonymous_usage_stats": False},
        }

        config = {
            "name": "test",
            "profile": "test",
            "flags": {"send_anonymous_usage_stats": False},
            "dbt-cloud": {
                "account_id": "123",
                "application": "test",
                "environment": "test",
                "api_key": "test",
            },
        }
        write_config_file(config, project.project_root, "dbt_project.yml")
        run_dbt(["parse"], expect_pass=True)
        conf = yaml.safe_load(
            Path(os.path.join(project.project_root, "dbt_project.yml")).read_text()
        )
        assert conf == config


class TestProjectDbtCloudConfigString:
    @pytest.fixture(scope="class")
    def models(self):
        return {"simple_model.sql": simple_model_sql, "simple_model.yml": simple_model_yml}

    def test_dbt_cloud_invalid(self, project):
        run_dbt()
        config = {"name": "test", "profile": "test", "dbt-cloud": "Some string"}
        update_config_file(config, "dbt_project.yml")
        expected_err = (
            "at path ['dbt-cloud']: 'Some string' is not valid under any of the given schemas"
        )
        with pytest.raises(ProjectContractError) as excinfo:
            run_dbt()
        assert expected_err in str(excinfo.value)


class TestVersionSpecifierChecksComeBeforeYamlValidation:
    def test_version_specifier_checks_before_yaml_validation(self, project) -> None:
        runner = dbtRunner()

        # if no version specifier error, we should get a yaml validation error
        config_update = {"this-is-not-a-valid-key": "my-value-for-invalid-key"}
        update_config_file(config_update, "dbt_project.yml")
        result = runner.invoke(["parse"])
        assert result.exception is not None
        assert isinstance(result.exception, ProjectContractError)
        assert "Additional properties are not allowed" in str(result.exception)

        # add bad version specifier, and assert we get the error for that
        update_config_file({"require-dbt-version": [">0.0.0", "<=0.0.1"]}, "dbt_project.yml")
        result = runner.invoke(["parse"])
        assert result.exception is not None
        assert isinstance(result.exception, DbtProjectError)
        assert "This version of dbt is not supported"


class TestArchiveNotAllowed:
    """At one point in time we supported an 'archive' key in projects, but no longer"""

    def test_archive_not_allowed(self, project):
        runner = dbtRunner()

        config_update = {
            "archive": {
                "source_schema": "a",
                "target_schema": "b",
                "tables": [
                    {
                        "source_table": "seed",
                        "target_table": "archive_actual",
                        "updated_at": "updated_at",
                        "unique_key": """id || '-' || first_name""",
                    },
                ],
            }
        }
        update_config_file(config_update, "dbt_project.yml")

        result = runner.invoke(["parse"])
        assert result.exception is not None
        assert isinstance(result.exception, ProjectContractError)
        assert "Additional properties are not allowed" in str(result.exception)
