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

        assert exc_info.value.count_packages_specified == 3
        assert exc_info.value.count_packages_installed == 2
        assert "nested_dependency" in exc_info.value.uninstalled_packages


class TestUninstalledPackagesErrorRaisedIfPackageLockDoesNotExist:
    """When package_lock.yml does not exist, error should be raised if packages.yml is non empty."""

    @pytest.fixture(scope="class")
    def packages(self):
        return {
            "packages": [
                {"package": "dbt-labs/dbt_utils", "version": "1.1.1"},
            ]
        }

    def test_error_raised_if_package_lock_does_not_exist(self, project):
        package_lock_path = Path(project.project_root) / "package-lock.yml"
        # ensure that package lock does not exist
        package_lock_path.unlink(missing_ok=True)

        with pytest.raises(UninstalledPackagesFoundError) as exc_info:
            run_dbt(["parse"])

        assert len(exc_info.value.uninstalled_packages) == 0
        assert exc_info.value.count_packages_specified == 1
        assert exc_info.value.count_packages_installed == 0


class TestNoErrorIfPackageYamlDoesNotExist:
    """When packages.yml does not exist, no error should be raised."""

    def test_no_error_raised_if_package_yml_does_not_exist(self, project):
        packages_yml_path = Path(project.project_root) / "packages.yml"
        packages_yml_path.unlink(missing_ok=True)

        run_dbt(["parse"])
