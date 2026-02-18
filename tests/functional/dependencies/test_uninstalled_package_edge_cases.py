import shutil
from pathlib import Path

import pytest

from dbt.exceptions import UninstalledPackagesFoundError
from dbt.tests.util import run_dbt


class TestUninstalledPackageWithNestedDependency:
    """When package_a and package_b are specified, package_b has a recursive dependency
    on package_c, and package_a is uninstalled (missing from dbt_packages),
    UninstalledPackagesFoundError should be raised.
    """

    @pytest.fixture(scope="class", autouse=True)
    def setUp(self, project):
        shutil.copytree(
            project.test_dir / Path("nested_dependency"),
            project.project_root / Path("nested_dependency"),
        )

    @pytest.fixture(scope="class")
    def packages(self):
        return {
            "packages": [
                {"package": "dbt-labs/dbt_utils", "version": "1.1.1"},
                {"local": "nested_dependency"},
            ]
        }

    def test_uninstalled_package_with_nested_dependency(self, project):
        run_dbt(["deps"])

        # Remove the local package by removing the symlink
        nested_dep_pkg = Path(project.project_root) / "dbt_packages" / "nested_dependency"
        nested_dep_pkg.unlink()

        with pytest.raises(UninstalledPackagesFoundError) as exc_info:
            run_dbt(["parse"])

        assert "nested_dependency" in exc_info.value.uninstalled_packages
