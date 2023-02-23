import pytest
from dbt.tests.util import run_dbt, update_config_file
from dbt.exceptions import ProjectContractError


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
