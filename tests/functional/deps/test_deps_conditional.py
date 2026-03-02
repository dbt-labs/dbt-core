"""Tests for conditional package enabling/disabling via the 'enabled' field.

Tests verify that:
- Packages with enabled: false are not installed
- Packages with enabled: true are installed normally
- Jinja expressions with env_var() are evaluated for the enabled field
- Jinja expressions with var() (CLI, vars.yml, dbt_project.yml) are evaluated
- Jinja expressions with target context are evaluated
- Disabled packages don't appear in lock file resolved deps
- Toggling enabled removes package from lock file
"""

import os
import shutil

import pytest
import yaml

from dbt.tests.util import run_dbt, write_file

model_sql = """
select 1 as id
"""


class TestPackageEnabledFalse:
    """Package with enabled: false is not installed."""

    @pytest.fixture(scope="class")
    def models(self):
        return {"test_model.sql": model_sql}

    @pytest.fixture(scope="class")
    def packages(self):
        return {
            "packages": [
                {
                    "package": "dbt-labs/dbt_utils",
                    "version": "1.3.0",
                    "enabled": False,
                },
            ]
        }

    @pytest.fixture
    def clean_start(self, project):
        if os.path.exists("dbt_packages"):
            shutil.rmtree("dbt_packages")
        if os.path.exists("package-lock.yml"):
            os.remove("package-lock.yml")

    def test_disabled_package_not_installed(self, project, clean_start):
        run_dbt(["deps"])
        # dbt_packages dir should not exist or be empty since only package is disabled
        if os.path.exists("dbt_packages"):
            assert len(os.listdir("dbt_packages")) == 0


class TestPackageEnabledTrue:
    """Package with enabled: true is installed normally."""

    @pytest.fixture(scope="class")
    def models(self):
        return {"test_model.sql": model_sql}

    @pytest.fixture(scope="class")
    def packages(self):
        return {
            "packages": [
                {
                    "package": "dbt-labs/dbt_utils",
                    "version": "1.3.0",
                    "enabled": True,
                },
            ]
        }

    @pytest.fixture
    def clean_start(self, project):
        if os.path.exists("dbt_packages"):
            shutil.rmtree("dbt_packages")
        if os.path.exists("package-lock.yml"):
            os.remove("package-lock.yml")

    def test_enabled_package_installed(self, project, clean_start):
        run_dbt(["deps"])
        assert os.path.exists("dbt_packages")
        assert "dbt_utils" in os.listdir("dbt_packages")


class TestPackageEnabledDefault:
    """Package without 'enabled' field defaults to enabled (backward compatible)."""

    @pytest.fixture(scope="class")
    def models(self):
        return {"test_model.sql": model_sql}

    @pytest.fixture(scope="class")
    def packages(self):
        return {
            "packages": [
                {
                    "package": "dbt-labs/dbt_utils",
                    "version": "1.3.0",
                },
            ]
        }

    @pytest.fixture
    def clean_start(self, project):
        if os.path.exists("dbt_packages"):
            shutil.rmtree("dbt_packages")
        if os.path.exists("package-lock.yml"):
            os.remove("package-lock.yml")

    def test_default_package_installed(self, project, clean_start):
        run_dbt(["deps"])
        assert os.path.exists("dbt_packages")
        assert "dbt_utils" in os.listdir("dbt_packages")


class TestPackageEnabledWithEnvVar:
    """Package with enabled using env_var() Jinja expression."""

    @pytest.fixture(scope="class")
    def models(self):
        return {"test_model.sql": model_sql}

    @pytest.fixture(scope="class")
    def packages(self):
        return {
            "packages": [
                {
                    "package": "dbt-labs/dbt_utils",
                    "version": "1.3.0",
                    "enabled": "{{ env_var('DBT_TEST_INSTALL_UTILS', 'false') == 'true' }}",
                },
            ]
        }

    @pytest.fixture
    def clean_start(self, project):
        if os.path.exists("dbt_packages"):
            shutil.rmtree("dbt_packages")
        if os.path.exists("package-lock.yml"):
            os.remove("package-lock.yml")

    def test_env_var_not_exists(self, project, clean_start, monkeypatch):
        """When env var is not set, package is not installed."""
        monkeypatch.delenv("DBT_TEST_INSTALL_UTILS", raising=False)
        run_dbt(["deps"])
        if os.path.exists("dbt_packages"):
            assert "dbt_utils" not in os.listdir("dbt_packages")

    def test_env_var_enabled(self, project, clean_start, monkeypatch):
        """When env var is 'true', package is installed."""
        monkeypatch.setenv("DBT_TEST_INSTALL_UTILS", "true")
        run_dbt(["deps"])
        assert os.path.exists("dbt_packages")
        assert "dbt_utils" in os.listdir("dbt_packages")

    def test_env_var_disabled(self, project, clean_start, monkeypatch):
        """When env var is 'false', package is not installed."""
        monkeypatch.setenv("DBT_TEST_INSTALL_UTILS", "false")
        run_dbt(["deps"])
        if os.path.exists("dbt_packages"):
            assert "dbt_utils" not in os.listdir("dbt_packages")


class TestPackageEnabledWithCliVar:
    """Package with enabled using var() from CLI --vars."""

    @pytest.fixture(scope="class")
    def models(self):
        return {"test_model.sql": model_sql}

    @pytest.fixture(scope="class")
    def packages(self):
        return {
            "packages": [
                {
                    "package": "dbt-labs/dbt_utils",
                    "version": "1.3.0",
                    "enabled": "{{ var('install_utils', true) }}",
                },
            ]
        }

    @pytest.fixture
    def clean_start(self, project):
        if os.path.exists("dbt_packages"):
            shutil.rmtree("dbt_packages")
        if os.path.exists("package-lock.yml"):
            os.remove("package-lock.yml")

    def test_var_default_enabled(self, project, clean_start):
        """When var defaults to true, package is installed."""
        run_dbt(["deps"])
        assert os.path.exists("dbt_packages")
        assert "dbt_utils" in os.listdir("dbt_packages")

    def test_var_disabled(self, project, clean_start):
        """When var is set to false via --vars, package is not installed."""
        run_dbt(["deps", "--vars", '{"install_utils": false}'])
        if os.path.exists("dbt_packages"):
            assert "dbt_utils" not in os.listdir("dbt_packages")


class TestPackageEnabledWithVarsYml:
    """Package with enabled using var() sourced from vars.yml."""

    @pytest.fixture(scope="class")
    def models(self):
        return {"test_model.sql": model_sql}

    @pytest.fixture(scope="class")
    def packages(self):
        return {
            "packages": [
                {
                    "package": "dbt-labs/dbt_utils",
                    "version": "1.3.0",
                    "enabled": "{{ var('install_utils', true) }}",
                },
            ]
        }

    @pytest.fixture(scope="class")
    def vars_yml_update(self):
        return {"vars": {"install_utils": False}}

    @pytest.fixture
    def clean_start(self, project):
        if os.path.exists("dbt_packages"):
            shutil.rmtree("dbt_packages")
        if os.path.exists("package-lock.yml"):
            os.remove("package-lock.yml")

    def test_vars_yml_disables_package(self, project, clean_start):
        """When vars.yml sets install_utils: false, package is not installed."""
        run_dbt(["deps"])
        if os.path.exists("dbt_packages"):
            assert "dbt_utils" not in os.listdir("dbt_packages")


class TestPackageEnabledWithProjectVars:
    """Package with enabled using var() sourced from dbt_project.yml vars."""

    @pytest.fixture(scope="class")
    def models(self):
        return {"test_model.sql": model_sql}

    @pytest.fixture(scope="class")
    def packages(self):
        return {
            "packages": [
                {
                    "package": "dbt-labs/dbt_utils",
                    "version": "1.3.0",
                    "enabled": "{{ var('install_utils', true) }}",
                },
            ]
        }

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"vars": {"install_utils": False}}

    @pytest.fixture
    def clean_start(self, project):
        if os.path.exists("dbt_packages"):
            shutil.rmtree("dbt_packages")
        if os.path.exists("package-lock.yml"):
            os.remove("package-lock.yml")

    def test_project_vars_disables_package(self, project, clean_start):
        """When dbt_project.yml vars sets install_utils: false, package is not installed."""
        run_dbt(["deps"])
        if os.path.exists("dbt_packages"):
            assert "dbt_utils" not in os.listdir("dbt_packages")


class TestPackageEnabledWithTarget:
    """Package with enabled using target context (target.name is 'default' in tests)."""

    @pytest.fixture(scope="class")
    def models(self):
        return {"test_model.sql": model_sql}

    @pytest.fixture(scope="class")
    def packages(self):
        return {
            "packages": [
                {
                    "package": "dbt-labs/dbt_utils",
                    "version": "1.3.0",
                    "enabled": "{{ target.name != 'prod' }}",
                },
            ]
        }

    @pytest.fixture
    def clean_start(self, project):
        if os.path.exists("dbt_packages"):
            shutil.rmtree("dbt_packages")
        if os.path.exists("package-lock.yml"):
            os.remove("package-lock.yml")

    def test_target_not_prod_enables_package(self, project, clean_start):
        """target.name is 'default' in tests, so != 'prod' is True -> enabled."""
        run_dbt(["deps"])
        assert os.path.exists("dbt_packages")
        assert "dbt_utils" in os.listdir("dbt_packages")


class TestPackageEnabledWithTargetDisabled:
    """Package disabled when target matches a specific name."""

    @pytest.fixture(scope="class")
    def models(self):
        return {"test_model.sql": model_sql}

    @pytest.fixture(scope="class")
    def packages(self):
        return {
            "packages": [
                {
                    "package": "dbt-labs/dbt_utils",
                    "version": "1.3.0",
                    "enabled": "{{ target.name == 'prod' }}",
                },
            ]
        }

    @pytest.fixture
    def clean_start(self, project):
        if os.path.exists("dbt_packages"):
            shutil.rmtree("dbt_packages")
        if os.path.exists("package-lock.yml"):
            os.remove("package-lock.yml")

    def test_target_not_prod_disables_package(self, project, clean_start):
        """target.name is 'default' in tests, so == 'prod' is False -> disabled."""
        run_dbt(["deps"])
        if os.path.exists("dbt_packages"):
            assert "dbt_utils" not in os.listdir("dbt_packages")


class TestDisabledPackageNotInLockFile:
    """Lock file only contains enabled packages, not disabled ones."""

    @pytest.fixture(scope="class")
    def models(self):
        return {"test_model.sql": model_sql}

    @pytest.fixture(scope="class")
    def packages(self):
        return {
            "packages": [
                {
                    "package": "dbt-labs/dbt_utils",
                    "version": "1.3.0",
                    "enabled": True,
                },
                {
                    "package": "dbt-labs/codegen",
                    "version": "0.12.1",
                    "enabled": False,
                },
            ]
        }

    @pytest.fixture
    def clean_start(self, project):
        if os.path.exists("dbt_packages"):
            shutil.rmtree("dbt_packages")
        if os.path.exists("package-lock.yml"):
            os.remove("package-lock.yml")

    def test_disabled_not_in_lock(self, project, clean_start):
        run_dbt(["deps", "--lock"])
        assert os.path.exists("package-lock.yml")
        with open("package-lock.yml") as f:
            lock_data = yaml.safe_load(f)
        packages = lock_data.get("packages", [])
        pkg_names = [p.get("package", p.get("name", "")) for p in packages]
        assert "dbt-labs/dbt_utils" in pkg_names or "dbt_utils" in pkg_names
        assert "dbt-labs/codegen" not in pkg_names
        assert "codegen" not in pkg_names


class TestToggleEnabledUpdatesLockFile:
    """Disabling a previously enabled package removes it from the lock file."""

    @pytest.fixture(scope="class")
    def models(self):
        return {"test_model.sql": model_sql}

    @pytest.fixture(scope="class")
    def packages(self):
        # Start with both packages enabled
        return {
            "packages": [
                {
                    "package": "dbt-labs/dbt_utils",
                    "version": "1.3.0",
                },
                {
                    "package": "dbt-labs/codegen",
                    "version": "0.12.1",
                },
            ]
        }

    @pytest.fixture
    def clean_start(self, project):
        if os.path.exists("dbt_packages"):
            shutil.rmtree("dbt_packages")
        if os.path.exists("package-lock.yml"):
            os.remove("package-lock.yml")

    def test_toggle_enabled_removes_from_lock(self, project, clean_start):
        # First: both packages enabled, lock file should have both
        run_dbt(["deps", "--lock"])
        assert os.path.exists("package-lock.yml")
        with open("package-lock.yml") as f:
            lock_data = yaml.safe_load(f)
        pkg_names = [p.get("package", p.get("name", "")) for p in lock_data["packages"]]
        assert any("codegen" in n for n in pkg_names), f"codegen should be in lock: {pkg_names}"

        # Now: rewrite packages.yml with codegen disabled
        new_packages = {
            "packages": [
                {
                    "package": "dbt-labs/dbt_utils",
                    "version": "1.3.0",
                },
                {
                    "package": "dbt-labs/codegen",
                    "version": "0.12.1",
                    "enabled": False,
                },
            ]
        }
        with open(os.path.join(project.project_root, "packages.yml"), "w") as f:
            yaml.safe_dump(new_packages, f)

        # Re-run deps --lock --upgrade to regenerate
        run_dbt(["deps", "--lock", "--upgrade"])
        with open("package-lock.yml") as f:
            lock_data = yaml.safe_load(f)
        pkg_names = [p.get("package", p.get("name", "")) for p in lock_data["packages"]]
        assert not any(
            "codegen" in n for n in pkg_names
        ), f"codegen should NOT be in lock: {pkg_names}"
        assert any(
            "dbt_utils" in n for n in pkg_names
        ), f"dbt_utils should be in lock: {pkg_names}"


class TestTransitiveDependencyScenarios:
    """Tests for transitive dependency behavior with the 'enabled' field.

    Setup:
    - Root project depends on local_pkg_b (controlled by var 'enable_local_pkg')
    - local_pkg_b depends on:
      - dbt-labs/dbt_utils (always enabled)
      - dbt-labs/codegen (controlled by var 'install_codegen')

    Scenarios:
    1. local_pkg_b disabled: nothing resolved or installed
    2. local_pkg_b enabled, codegen disabled: only local_pkg_b + dbt_utils installed
    3. local_pkg_b enabled, codegen enabled: local_pkg_b + dbt_utils + codegen installed
    """

    @pytest.fixture(scope="class")
    def models(self):
        return {"test_model.sql": model_sql}

    @pytest.fixture(scope="class")
    def local_pkg_b(self, project):
        """Create a local package with two transitive deps."""
        pkg_dir = os.path.join(project.project_root, "local_pkg_b")
        os.makedirs(pkg_dir, exist_ok=True)

        write_file(
            yaml.safe_dump({"name": "local_pkg_b", "version": "1.0.0"}),
            pkg_dir,
            "dbt_project.yml",
        )

        # Two transitive deps: dbt_utils (always) and codegen (var-controlled)
        write_file(
            yaml.safe_dump(
                {
                    "packages": [
                        {"package": "dbt-labs/dbt_utils", "version": "1.3.0"},
                        {
                            "package": "dbt-labs/codegen",
                            "version": "0.12.1",
                            "enabled": "{{ var('install_codegen', true) }}",
                        },
                    ]
                }
            ),
            pkg_dir,
            "packages.yml",
        )

        return pkg_dir

    @pytest.fixture(scope="class")
    def packages(self):
        return {
            "packages": [
                {
                    "local": "local_pkg_b",
                    "enabled": "{{ var('enable_local_pkg', true) }}",
                },
            ]
        }

    @pytest.fixture
    def clean_start(self, project):
        if os.path.exists("dbt_packages"):
            shutil.rmtree("dbt_packages")
        if os.path.exists("package-lock.yml"):
            os.remove("package-lock.yml")

    def test_local_pkg_disabled(self, project, local_pkg_b, clean_start):
        """When local_pkg_b is disabled, nothing is resolved or installed."""
        run_dbt(["deps", "--vars", '{"enable_local_pkg": false}'])

        # dbt_packages should be empty
        if os.path.exists("dbt_packages"):
            assert len(os.listdir("dbt_packages")) == 0

        # No lock file created when all packages are disabled
        assert not os.path.exists("package-lock.yml")

    def test_transitive_codegen_disabled(self, project, local_pkg_b, clean_start):
        """When codegen is disabled via var, only local_pkg_b + dbt_utils are installed."""
        run_dbt(["deps", "--vars", '{"install_codegen": false}'])

        assert os.path.exists("dbt_packages")
        installed = os.listdir("dbt_packages")
        assert "local_pkg_b" in installed
        assert "dbt_utils" in installed
        assert "codegen" not in installed

        # Lock file should contain local_pkg_b and dbt_utils, but not codegen
        assert os.path.exists("package-lock.yml")
        with open("package-lock.yml") as f:
            lock_data = yaml.safe_load(f)
        pkg_names = [p.get("package", p.get("name", "")) for p in lock_data["packages"]]
        assert any("dbt_utils" in n for n in pkg_names)
        assert not any("codegen" in n for n in pkg_names)

    def test_all_enabled(self, project, local_pkg_b, clean_start):
        """When all vars default to true, both transitive deps are installed."""
        run_dbt(["deps", "--vars", '{"install_codegen": true}'])

        assert os.path.exists("dbt_packages")
        installed = os.listdir("dbt_packages")
        assert "local_pkg_b" in installed
        assert "dbt_utils" in installed
        assert "codegen" in installed

        # Lock file should contain all three
        assert os.path.exists("package-lock.yml")
        with open("package-lock.yml") as f:
            lock_data = yaml.safe_load(f)
        pkg_names = [p.get("package", p.get("name", "")) for p in lock_data["packages"]]
        assert any("dbt_utils" in n for n in pkg_names)
        assert any("codegen" in n for n in pkg_names)
