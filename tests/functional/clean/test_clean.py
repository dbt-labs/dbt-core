import pytest

from dbt.exceptions import DbtRuntimeError
from dbt.tests.util import run_dbt


class TestCleanSourcePath:
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return "clean-targets: ['models']"

    def test_clean_source_path(self, project):
        with pytest.raises(DbtRuntimeError, match="dbt will not clean the following source paths"):
            run_dbt(["clean"])


class TestCleanPathOutsideProjectRelative:
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return "clean-targets: ['..']"

    def test_clean_path_outside_project(self, project):
        with pytest.raises(
            DbtRuntimeError,
            match="dbt will not clean the following directories outside the project",
        ):
            run_dbt(["clean"])


class TestCleanPathOutsideProjectAbsolute:
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return "clean-targets: ['/']"

    def test_clean_path_outside_project(self, project):
        with pytest.raises(
            DbtRuntimeError,
            match="dbt will not clean the following directories outside the project",
        ):
            run_dbt(["clean"])


class TestCleanPathOutsideProjectWithFlag:
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return "clean-targets: ['/tmp/foo']"

    def test_clean_path_outside_project(self, project):
        # Doesn't fail because flag is set
        run_dbt(["clean", "--no-clean-project-files-only"])

        with pytest.raises(
            DbtRuntimeError,
            match="dbt will not clean the following directories outside the project",
        ):
            run_dbt(["clean", "--clean-project-files-only"])
