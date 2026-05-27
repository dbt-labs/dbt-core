from pathlib import Path

import pytest

from dbt.exceptions import DbtRuntimeError
from dbt.tests.util import run_dbt
from tests.functional.utils import up_one


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


class TestCleanRelativeProjectDir:
    def test_clean_relative_project_dir(self, project):
        with up_one():
            project_dir = Path(project.project_root).relative_to(Path.cwd())
            run_dbt(["clean", "--project-dir", str(project_dir)])


class TestCleanHonorsTargetPathFlag:
    """Regression test for dbt-labs/dbt-core#11346.

    When ``--target-path`` is passed (or ``DBT_TARGET_PATH`` is set), the
    overridden target dir should be cleaned even if the user has explicitly
    set ``clean-targets`` in ``dbt_project.yml`` (so that the override does
    not silently get ignored).
    """

    @pytest.fixture(scope="class")
    def project_config_update(self):
        # Explicitly define clean-targets WITHOUT the overridden target path.
        # Pre-fix, dbt clean --target-path my_target would leave my_target/
        # behind on disk.
        return "clean-targets: ['target']"

    def test_clean_target_path_flag(self, project):
        custom_target = Path(project.project_root) / "my_custom_target"

        # Populate the custom target directory by parsing into it.
        run_dbt(["parse", "--target-path", str(custom_target)])
        assert custom_target.exists(), "custom target dir should exist after parse"

        # Now clean using the same overridden target path. Pre-fix this was a
        # no-op for the custom path; post-fix the dir must be removed.
        run_dbt(["clean", "--target-path", str(custom_target)])
        assert (
            not custom_target.exists()
        ), "dbt clean must remove the directory pointed to by --target-path"
