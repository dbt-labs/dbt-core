from pathlib import Path

import pytest

from dbt.tests.util import run_dbt


class TestPackagesInstallPathConfig:
    @pytest.fixture(scope="class")
    def packages(self):
        return {
            "packages": [
                {
                    "package": "fivetran/fivetran_utils",
                    "version": "0.4.7",
                },
            ]
        }

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"config-version": 2, "packages-install-path": "project_dbt_packages"}

    def test_packages_install_path(self, project):
        run_dbt(["deps"])
        assert Path("project_dbt_packages").is_dir()
        assert not Path("dbt_packages").is_dir()


class TestPackagesInstallPathEnvVar:
    def test_packages_install_path(self, project, monkeypatch):
        monkeypatch.setenv("DBT_PACKAGES_INSTALL_PATH", "env_dbt_packages")
        run_dbt(["deps"])
        assert Path("env_dbt_packages").is_dir()
        assert not Path("project_dbt_packages").is_dir()
        assert not Path("dbt_packages").is_dir()


class TestPackagesInstallPathCliArg:
    def test_packages_install_path(self, project, monkeypatch):
        monkeypatch.setenv("DBT_PACKAGES_INSTALL_PATH", "env_dbt_packages")
        run_dbt(["deps", "--packages-install-path", "cli_dbt_packages"])
        assert Path("cli_dbt_packages").is_dir()
        assert not Path("env_dbt_packages").is_dir()
        assert not Path("project_dbt_packages").is_dir()
        assert not Path("dbt_packages").is_dir()
