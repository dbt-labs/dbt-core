"""Tests for conditional package enabling/disabling and re-lock behavior.

Part 1 — Basic enabled field:
- Static enabled: false / true / omitted (backward compat)
- env_var default fallback, env_var explicitly false
- var from vars.yml
- Disabled package excluded from lock file
- Toggling enabled in packages.yml triggers sha1-based re-lock

Part 2 — Re-lock on rendering context change (root-level):
1. var from dbt_project.yml changes between runs -> re-lock
2. var overridden via --vars CLI changes between runs -> re-lock
3. env_var changes between runs -> re-lock
4. target changes between runs -> re-lock

Part 3 — Re-lock on rendering context change (transitive):
5. transitive dep uses var -> re-lock on change
6. transitive dep uses CLI var -> re-lock on change
7. transitive dep uses env_var -> re-lock on change
8. transitive dep uses target -> re-lock on change
9. transitive root package disabled -> nothing installed

Part 4 — No re-lock when unrelated context changes:
10. root: unrelated var change -> no re-lock
11. root: unrelated env_var change -> no re-lock
12. transitive: unrelated var change -> no re-lock
13. transitive: unrelated env_var change -> no re-lock
"""

import os
import shutil
from copy import deepcopy

import pytest
import yaml

from dbt.tests.util import run_dbt, run_dbt_and_capture, write_file

model_sql = """
select 1 as id
"""


def _read_lock():
    with open("package-lock.yml") as f:
        return yaml.safe_load(f)


def _lock_pkg_names(lock_data):
    return [p.get("package", p.get("name", "")) for p in lock_data["packages"]]


def _lock_has_rendering_context(lock_data):
    return "rendering_context" in lock_data


# ===========================================================================
# Part 1: Basic enabled field behavior
# ===========================================================================


class TestPackageEnabledFalse:
    """Package with static enabled: false is not installed."""

    @pytest.fixture(scope="class")
    def models(self):
        return {"test_model.sql": model_sql}

    @pytest.fixture(scope="class")
    def packages(self):
        return {
            "packages": [
                {"package": "dbt-labs/dbt_utils", "version": "1.3.0", "enabled": False},
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
        assert not os.path.exists("dbt_packages")
        assert not os.path.exists("package-lock.yml")


class TestPackageEnabledTrue:
    """Package with static enabled: true is installed normally."""

    @pytest.fixture(scope="class")
    def models(self):
        return {"test_model.sql": model_sql}

    @pytest.fixture(scope="class")
    def packages(self):
        return {
            "packages": [
                {"package": "dbt-labs/dbt_utils", "version": "1.3.0", "enabled": True},
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
                {"package": "dbt-labs/dbt_utils", "version": "1.3.0"},
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


class TestPackageEnabledWithEnvVars:
    """env_var() edge cases: missing env var falls back to default, explicit false disables."""

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

    def test_env_var_not_set_uses_default(self, project, clean_start, monkeypatch):
        """When env var is not set, default 'false' is used -> not installed."""
        monkeypatch.delenv("DBT_TEST_INSTALL_UTILS", raising=False)
        run_dbt(["deps"])
        assert not os.path.exists("dbt_packages")

    def test_env_var_explicitly_false(self, project, clean_start, monkeypatch):
        """When env var is explicitly 'false' -> not installed."""
        monkeypatch.setenv("DBT_TEST_INSTALL_UTILS", "false")
        run_dbt(["deps"])
        assert not os.path.exists("dbt_packages")

    def test_env_var_explicitly_true(self, project, clean_start, monkeypatch):
        """When env var is explicitly 'true' -> installed."""
        monkeypatch.setenv("DBT_TEST_INSTALL_UTILS", "true")
        run_dbt(["deps"])
        assert "dbt_utils" in os.listdir("dbt_packages")


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
        assert not os.path.exists("dbt_packages")


class TestDisabledPackageNotInLockFile:
    """Lock file only contains enabled packages, not disabled ones."""

    @pytest.fixture(scope="class")
    def models(self):
        return {"test_model.sql": model_sql}

    @pytest.fixture(scope="class")
    def packages(self):
        return {
            "packages": [
                {"package": "dbt-labs/dbt_utils", "version": "1.3.0", "enabled": True},
                {"package": "dbt-labs/codegen", "version": "0.12.1", "enabled": False},
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
        lock_data = _read_lock()
        pkg_names = _lock_pkg_names(lock_data)
        assert any("dbt_utils" in n for n in pkg_names)
        assert not any("codegen" in n for n in pkg_names)


class TestToggleEnabledUpdatesLockFile:
    """Disabling a previously enabled package via packages.yml rewrite triggers re-lock."""

    @pytest.fixture(scope="class")
    def models(self):
        return {"test_model.sql": model_sql}

    @pytest.fixture(scope="class")
    def packages(self):
        return {
            "packages": [
                {"package": "dbt-labs/dbt_utils", "version": "1.3.0"},
                {"package": "dbt-labs/codegen", "version": "0.12.1"},
            ]
        }

    @pytest.fixture
    def clean_start(self, project):
        if os.path.exists("dbt_packages"):
            shutil.rmtree("dbt_packages")
        if os.path.exists("package-lock.yml"):
            os.remove("package-lock.yml")

    def test_toggle_enabled_removes_from_lock(self, project, clean_start):
        # First: both enabled
        run_dbt(["deps"])
        assert "codegen" in os.listdir("dbt_packages")
        assert "dbt_utils" in os.listdir("dbt_packages")
        lock_data = _read_lock()
        pkg_names = _lock_pkg_names(lock_data)
        assert any("codegen" in n for n in pkg_names)

        # Rewrite packages.yml with codegen disabled
        new_packages = {
            "packages": [
                {"package": "dbt-labs/dbt_utils", "version": "1.3.0"},
                {"package": "dbt-labs/codegen", "version": "0.12.1", "enabled": False},
            ]
        }
        with open(os.path.join(project.project_root, "packages.yml"), "w") as f:
            yaml.safe_dump(new_packages, f)

        # Plain dbt deps detects sha1 hash change -> re-lock, codegen removed
        run_dbt(["deps"])
        assert "codegen" not in os.listdir("dbt_packages")
        assert "dbt_utils" in os.listdir("dbt_packages")
        lock_data = _read_lock()
        pkg_names = _lock_pkg_names(lock_data)
        assert not any("codegen" in n for n in pkg_names)
        assert any("dbt_utils" in n for n in pkg_names)


# ===========================================================================
# Part 2: Re-lock on rendering context change (root-level)
# ===========================================================================


# ---------------------------------------------------------------------------
# Root-level: var from dbt_project.yml
# ---------------------------------------------------------------------------


class TestRootRelockOnProjectVarChange:
    """Root package uses var() from dbt_project.yml.

    Run 1: var('install_utils') = true  -> dbt_utils installed
    Run 2: change var to false in dbt_project.yml -> re-lock, dbt_utils removed
    """

    @pytest.fixture(scope="class")
    def models(self):
        return {"test_model.sql": model_sql}

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"vars": {"install_utils": True}}

    @pytest.fixture(scope="class")
    def packages(self):
        return {
            "packages": [
                {
                    "package": "dbt-labs/dbt_utils",
                    "version": "1.3.0",
                    "enabled": "{{ var('install_utils') }}",
                },
            ]
        }

    @pytest.fixture
    def clean_start(self, project):
        if os.path.exists("dbt_packages"):
            shutil.rmtree("dbt_packages")
        if os.path.exists("package-lock.yml"):
            os.remove("package-lock.yml")

    def test_relock_on_project_var_change(self, project, clean_start):
        # Run 1: install_utils=true -> package installed
        run_dbt(["deps"])
        assert "dbt_utils" in os.listdir("dbt_packages")
        lock1 = _read_lock()
        assert any("dbt_utils" in n for n in _lock_pkg_names(lock1))

        # Change dbt_project.yml var to false
        write_file(
            yaml.safe_dump(
                {
                    "name": "test",
                    "profile": "test",
                    "flags": {"send_anonymous_usage_stats": False},
                    "vars": {"install_utils": False},
                }
            ),
            project.project_root,
            "dbt_project.yml",
        )

        # Run 2: all packages disabled -> lock() returns False, lock file removed,
        # stale dbt_packages cleaned up
        run_dbt(["deps"])
        assert not os.path.exists("dbt_packages")
        assert not os.path.exists("package-lock.yml")


# ---------------------------------------------------------------------------
# Root-level: var overridden via --vars CLI
# ---------------------------------------------------------------------------


class TestRootRelockOnCliVarChange:
    """Root package uses var(). CLI --vars override changes between runs.

    Run 1: --vars '{"install_utils": true}'  -> dbt_utils installed
    Run 2: --vars '{"install_utils": false}' -> re-lock, dbt_utils removed
    """

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

    def test_relock_on_cli_var_change(self, project, clean_start):
        # Run 1: CLI var true -> installed
        run_dbt(["deps", "--vars", '{"install_utils": true}'])
        assert "dbt_utils" in os.listdir("dbt_packages")
        lock1 = _read_lock()
        assert any("dbt_utils" in n for n in _lock_pkg_names(lock1))

        # Run 2: all packages disabled -> lock file removed, dbt_packages cleaned
        run_dbt(["deps", "--vars", '{"install_utils": false}'])
        assert not os.path.exists("dbt_packages")
        assert not os.path.exists("package-lock.yml")


# ---------------------------------------------------------------------------
# Root-level: env_var
# ---------------------------------------------------------------------------


class TestRootRelockOnEnvVarChange:
    """Root package uses env_var(). If env var changes between runs, re-lock.

    Run 1: ENABLE_UTILS=true  -> dbt_utils installed
    Run 2: ENABLE_UTILS=false -> re-lock, dbt_utils removed
    """

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
                    "enabled": "{{ env_var('DBT_TEST_ENABLE_UTILS', 'true') == 'true' }}",
                },
            ]
        }

    @pytest.fixture
    def clean_start(self, project):
        if os.path.exists("dbt_packages"):
            shutil.rmtree("dbt_packages")
        if os.path.exists("package-lock.yml"):
            os.remove("package-lock.yml")

    def test_relock_on_env_var_change(self, project, clean_start, monkeypatch):
        # Run 1: env var true -> installed
        monkeypatch.setenv("DBT_TEST_ENABLE_UTILS", "true")
        run_dbt(["deps"])
        assert "dbt_utils" in os.listdir("dbt_packages")
        lock1 = _read_lock()
        assert any("dbt_utils" in n for n in _lock_pkg_names(lock1))

        # Run 2: all packages disabled -> lock file removed, dbt_packages cleaned
        monkeypatch.setenv("DBT_TEST_ENABLE_UTILS", "false")
        run_dbt(["deps"])
        assert not os.path.exists("dbt_packages")
        assert not os.path.exists("package-lock.yml")


# ---------------------------------------------------------------------------
# Root-level: target
# ---------------------------------------------------------------------------


class TestRootRelockOnTargetChange:
    """Root package uses target.name. If target changes between runs, re-lock.

    Run 1: --target default (target.name != 'prod' -> True)  -> installed
    Run 2: --target prod    (target.name != 'prod' -> False) -> re-lock, removed
    """

    @pytest.fixture(scope="class")
    def models(self):
        return {"test_model.sql": model_sql}

    @pytest.fixture(scope="class")
    def profiles_config_update(self, dbt_profile_target, unique_schema):
        default_target = deepcopy(dbt_profile_target)
        default_target["schema"] = unique_schema
        prod_target = deepcopy(dbt_profile_target)
        prod_target["schema"] = unique_schema
        return {
            "test": {
                "outputs": {
                    "default": default_target,
                    "prod": prod_target,
                },
                "target": "default",
            }
        }

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

    def test_relock_on_target_change(self, project, clean_start):
        # Run 1: default target -> installed
        run_dbt(["deps", "--target", "default"])
        assert "dbt_utils" in os.listdir("dbt_packages")
        lock1 = _read_lock()
        assert any("dbt_utils" in n for n in _lock_pkg_names(lock1))

        # Run 2: all packages disabled -> lock file removed, dbt_packages cleaned
        run_dbt(["deps", "--target", "prod"])
        assert not os.path.exists("dbt_packages")
        assert not os.path.exists("package-lock.yml")


# ===========================================================================
# Part 3: Re-lock on rendering context change (transitive)
# ===========================================================================


# ---------------------------------------------------------------------------
# Transitive: root package disabled -> nothing installed
# ---------------------------------------------------------------------------


class TestTransitiveRootDisabled:
    """When root local package is disabled, nothing is resolved or installed."""

    @pytest.fixture(scope="class")
    def models(self):
        return {"test_model.sql": model_sql}

    @pytest.fixture(scope="class")
    def local_pkg(self, project):
        pkg_dir = os.path.join(project.project_root, "local_pkg")
        os.makedirs(pkg_dir, exist_ok=True)
        write_file(
            yaml.safe_dump({"name": "local_pkg", "version": "1.0.0"}),
            pkg_dir,
            "dbt_project.yml",
        )
        write_file(
            yaml.safe_dump({"packages": [{"package": "dbt-labs/dbt_utils", "version": "1.3.0"}]}),
            pkg_dir,
            "packages.yml",
        )
        return pkg_dir

    @pytest.fixture(scope="class")
    def packages(self):
        return {
            "packages": [
                {
                    "local": "local_pkg",
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

    def test_root_disabled_nothing_installed(self, project, local_pkg, clean_start):
        run_dbt(["deps", "--vars", '{"enable_local_pkg": false}'])
        assert not os.path.exists("dbt_packages")
        assert not os.path.exists("package-lock.yml")


# ---------------------------------------------------------------------------
# Transitive: var from dbt_project.yml
# ---------------------------------------------------------------------------


class TestTransitiveRelockOnProjectVarChange:
    """Transitive dep uses var(). If project var changes, re-lock.

    Setup: root -> local_pkg -> dbt_utils (controlled by var 'install_utils')
    Run 1: install_utils=true  -> dbt_utils installed
    Run 2: change var to false -> re-lock, dbt_utils removed
    """

    @pytest.fixture(scope="class")
    def models(self):
        return {"test_model.sql": model_sql}

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"vars": {"install_utils": True}}

    @pytest.fixture(scope="class")
    def local_pkg(self, project):
        pkg_dir = os.path.join(project.project_root, "local_pkg")
        os.makedirs(pkg_dir, exist_ok=True)
        write_file(
            yaml.safe_dump({"name": "local_pkg", "version": "1.0.0"}),
            pkg_dir,
            "dbt_project.yml",
        )
        write_file(
            yaml.safe_dump(
                {
                    "packages": [
                        {
                            "package": "dbt-labs/dbt_utils",
                            "version": "1.3.0",
                            "enabled": "{{ var('install_utils') }}",
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
        return {"packages": [{"local": "local_pkg"}]}

    @pytest.fixture
    def clean_start(self, project):
        if os.path.exists("dbt_packages"):
            shutil.rmtree("dbt_packages")
        if os.path.exists("package-lock.yml"):
            os.remove("package-lock.yml")

    def test_relock_on_transitive_project_var_change(self, project, local_pkg, clean_start):
        # Run 1: install_utils=true -> dbt_utils installed as transitive dep
        run_dbt(["deps"])
        installed = os.listdir("dbt_packages")
        assert "local_pkg" in installed
        assert "dbt_utils" in installed
        lock1 = _read_lock()
        assert any("dbt_utils" in n for n in _lock_pkg_names(lock1))
        assert "install_utils" in lock1["rendering_context"]["var_names"]
        old_hash = lock1["rendering_context"]["hash"]

        # Change dbt_project.yml var to false
        write_file(
            yaml.safe_dump(
                {
                    "name": "test",
                    "profile": "test",
                    "flags": {"send_anonymous_usage_stats": False},
                    "vars": {"install_utils": False},
                }
            ),
            project.project_root,
            "dbt_project.yml",
        )

        # Run 2: install_utils=false -> re-lock, dbt_utils removed
        run_dbt(["deps"])
        installed = os.listdir("dbt_packages")
        assert "local_pkg" in installed
        assert "dbt_utils" not in installed

        lock2 = _read_lock()
        assert not any("dbt_utils" in n for n in _lock_pkg_names(lock2))
        assert lock2["rendering_context"]["hash"] != old_hash


# ---------------------------------------------------------------------------
# Transitive: var overridden via --vars CLI
# ---------------------------------------------------------------------------


class TestTransitiveRelockOnCliVarChange:
    """Transitive dep uses var(). CLI --vars override changes between runs.

    Setup: root -> local_pkg -> dbt_utils (controlled by var 'install_utils')
    Run 1: --vars '{"install_utils": true}'  -> dbt_utils installed
    Run 2: --vars '{"install_utils": false}' -> re-lock, dbt_utils removed
    """

    @pytest.fixture(scope="class")
    def models(self):
        return {"test_model.sql": model_sql}

    @pytest.fixture(scope="class")
    def local_pkg(self, project):
        pkg_dir = os.path.join(project.project_root, "local_pkg")
        os.makedirs(pkg_dir, exist_ok=True)
        write_file(
            yaml.safe_dump({"name": "local_pkg", "version": "1.0.0"}),
            pkg_dir,
            "dbt_project.yml",
        )
        write_file(
            yaml.safe_dump(
                {
                    "packages": [
                        {
                            "package": "dbt-labs/dbt_utils",
                            "version": "1.3.0",
                            "enabled": "{{ var('install_utils', true) }}",
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
        return {"packages": [{"local": "local_pkg"}]}

    @pytest.fixture
    def clean_start(self, project):
        if os.path.exists("dbt_packages"):
            shutil.rmtree("dbt_packages")
        if os.path.exists("package-lock.yml"):
            os.remove("package-lock.yml")

    def test_relock_on_transitive_cli_var_change(self, project, local_pkg, clean_start):
        # Run 1: CLI var true -> installed
        run_dbt(["deps", "--vars", '{"install_utils": true}'])
        installed = os.listdir("dbt_packages")
        assert "local_pkg" in installed
        assert "dbt_utils" in installed
        lock1 = _read_lock()
        assert any("dbt_utils" in n for n in _lock_pkg_names(lock1))
        old_hash = lock1["rendering_context"]["hash"]

        # Run 2: CLI var false -> re-lock, removed
        run_dbt(["deps", "--vars", '{"install_utils": false}'])
        installed = os.listdir("dbt_packages")
        assert "local_pkg" in installed
        assert "dbt_utils" not in installed

        lock2 = _read_lock()
        assert not any("dbt_utils" in n for n in _lock_pkg_names(lock2))
        assert lock2["rendering_context"]["hash"] != old_hash


# ---------------------------------------------------------------------------
# Transitive: env_var
# ---------------------------------------------------------------------------


class TestTransitiveRelockOnEnvVarChange:
    """Transitive dep uses env_var(). If env var changes, re-lock.

    Setup: root -> local_pkg -> dbt_utils (controlled by env_var)
    Run 1: DBT_TEST_ENABLE_UTILS=true  -> dbt_utils installed
    Run 2: DBT_TEST_ENABLE_UTILS=false -> re-lock, dbt_utils removed
    """

    @pytest.fixture(scope="class")
    def models(self):
        return {"test_model.sql": model_sql}

    @pytest.fixture(scope="class")
    def local_pkg(self, project):
        pkg_dir = os.path.join(project.project_root, "local_pkg")
        os.makedirs(pkg_dir, exist_ok=True)
        write_file(
            yaml.safe_dump({"name": "local_pkg", "version": "1.0.0"}),
            pkg_dir,
            "dbt_project.yml",
        )
        write_file(
            yaml.safe_dump(
                {
                    "packages": [
                        {
                            "package": "dbt-labs/dbt_utils",
                            "version": "1.3.0",
                            "enabled": "{{ env_var('DBT_TEST_ENABLE_UTILS', 'true') == 'true' }}",
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
        return {"packages": [{"local": "local_pkg"}]}

    @pytest.fixture
    def clean_start(self, project):
        if os.path.exists("dbt_packages"):
            shutil.rmtree("dbt_packages")
        if os.path.exists("package-lock.yml"):
            os.remove("package-lock.yml")

    def test_relock_on_transitive_env_var_change(
        self, project, local_pkg, clean_start, monkeypatch
    ):
        # Run 1: env var true -> installed
        monkeypatch.setenv("DBT_TEST_ENABLE_UTILS", "true")
        run_dbt(["deps"])
        installed = os.listdir("dbt_packages")
        assert "local_pkg" in installed
        assert "dbt_utils" in installed
        lock1 = _read_lock()
        assert any("dbt_utils" in n for n in _lock_pkg_names(lock1))
        assert "DBT_TEST_ENABLE_UTILS" in lock1["rendering_context"]["env_var_names"]
        old_hash = lock1["rendering_context"]["hash"]

        # Run 2: env var false -> re-lock, removed
        monkeypatch.setenv("DBT_TEST_ENABLE_UTILS", "false")
        run_dbt(["deps"])
        installed = os.listdir("dbt_packages")
        assert "local_pkg" in installed
        assert "dbt_utils" not in installed

        lock2 = _read_lock()
        assert not any("dbt_utils" in n for n in _lock_pkg_names(lock2))
        assert lock2["rendering_context"]["hash"] != old_hash


# ---------------------------------------------------------------------------
# Transitive: target
# ---------------------------------------------------------------------------


class TestTransitiveRelockOnTargetChange:
    """Transitive dep uses target.name. If target changes, re-lock.

    Setup: root -> local_pkg -> dbt_utils (controlled by target.name)
    Run 1: --target default (name != 'prod' -> True)  -> installed
    Run 2: --target prod    (name != 'prod' -> False) -> re-lock, removed
    """

    @pytest.fixture(scope="class")
    def models(self):
        return {"test_model.sql": model_sql}

    @pytest.fixture(scope="class")
    def profiles_config_update(self, dbt_profile_target, unique_schema):
        default_target = deepcopy(dbt_profile_target)
        default_target["schema"] = unique_schema
        prod_target = deepcopy(dbt_profile_target)
        prod_target["schema"] = unique_schema
        return {
            "test": {
                "outputs": {
                    "default": default_target,
                    "prod": prod_target,
                },
                "target": "default",
            }
        }

    @pytest.fixture(scope="class")
    def local_pkg(self, project):
        pkg_dir = os.path.join(project.project_root, "local_pkg")
        os.makedirs(pkg_dir, exist_ok=True)
        write_file(
            yaml.safe_dump({"name": "local_pkg", "version": "1.0.0"}),
            pkg_dir,
            "dbt_project.yml",
        )
        write_file(
            yaml.safe_dump(
                {
                    "packages": [
                        {
                            "package": "dbt-labs/dbt_utils",
                            "version": "1.3.0",
                            "enabled": "{{ target.name != 'prod' }}",
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
        return {"packages": [{"local": "local_pkg"}]}

    @pytest.fixture
    def clean_start(self, project):
        if os.path.exists("dbt_packages"):
            shutil.rmtree("dbt_packages")
        if os.path.exists("package-lock.yml"):
            os.remove("package-lock.yml")

    def test_relock_on_transitive_target_change(self, project, local_pkg, clean_start):
        # Run 1: default target -> installed
        run_dbt(["deps", "--target", "default"])
        installed = os.listdir("dbt_packages")
        assert "local_pkg" in installed
        assert "dbt_utils" in installed
        lock1 = _read_lock()
        assert any("dbt_utils" in n for n in _lock_pkg_names(lock1))
        assert "name" in lock1["rendering_context"]["target_keys"]
        old_hash = lock1["rendering_context"]["hash"]

        # Run 2: prod target -> re-lock, removed
        run_dbt(["deps", "--target", "prod"])
        installed = os.listdir("dbt_packages")
        assert "local_pkg" in installed
        assert "dbt_utils" not in installed

        lock2 = _read_lock()
        assert not any("dbt_utils" in n for n in _lock_pkg_names(lock2))
        assert lock2["rendering_context"]["hash"] != old_hash


# ===========================================================================
# Part 4: No re-lock when unrelated context changes
# ===========================================================================


# ---------------------------------------------------------------------------
# Root-level: unrelated var change does NOT trigger re-lock
# ---------------------------------------------------------------------------


class TestRootNoRelockOnUnrelatedVarChange:
    """Package uses var('install_utils'). Changing a different var should not re-lock."""

    @pytest.fixture(scope="class")
    def models(self):
        return {"test_model.sql": model_sql}

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"vars": {"install_utils": True, "unrelated_var": "hello"}}

    @pytest.fixture(scope="class")
    def packages(self):
        return {
            "packages": [
                {
                    "package": "dbt-labs/dbt_utils",
                    "version": "1.3.0",
                    "enabled": "{{ var('install_utils') }}",
                },
            ]
        }

    @pytest.fixture
    def clean_start(self, project):
        if os.path.exists("dbt_packages"):
            shutil.rmtree("dbt_packages")
        if os.path.exists("package-lock.yml"):
            os.remove("package-lock.yml")

    def test_unrelated_var_no_relock(self, project, clean_start):
        # Run 1: install_utils=true -> installed, lock created
        run_dbt(["deps"])
        assert "dbt_utils" in os.listdir("dbt_packages")
        lock1 = _read_lock()
        assert any("dbt_utils" in n for n in _lock_pkg_names(lock1))

        # Change unrelated var in dbt_project.yml (install_utils stays true)
        write_file(
            yaml.safe_dump(
                {
                    "name": "test",
                    "profile": "test",
                    "flags": {"send_anonymous_usage_stats": False},
                    "vars": {"install_utils": True, "unrelated_var": "changed"},
                }
            ),
            project.project_root,
            "dbt_project.yml",
        )

        # Run 2: no re-lock, dbt_utils still installed
        _, stdout = run_dbt_and_capture(["deps"])
        assert "Updating lock file" not in stdout
        assert "dbt_utils" in os.listdir("dbt_packages")


# ---------------------------------------------------------------------------
# Root-level: unrelated env_var change does NOT trigger re-lock
# ---------------------------------------------------------------------------


class TestRootNoRelockOnUnrelatedEnvVarChange:
    """Package uses env_var('DBT_TEST_ENABLE_UTILS'). Changing a different env var should not re-lock."""

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
                    "enabled": "{{ env_var('DBT_TEST_ENABLE_UTILS', 'true') == 'true' }}",
                },
            ]
        }

    @pytest.fixture
    def clean_start(self, project):
        if os.path.exists("dbt_packages"):
            shutil.rmtree("dbt_packages")
        if os.path.exists("package-lock.yml"):
            os.remove("package-lock.yml")

    def test_unrelated_env_var_no_relock(self, project, clean_start, monkeypatch):
        # Run 1: DBT_TEST_ENABLE_UTILS=true -> installed
        monkeypatch.setenv("DBT_TEST_ENABLE_UTILS", "true")
        monkeypatch.setenv("DBT_UNRELATED_VAR", "foo")
        run_dbt(["deps"])
        assert "dbt_utils" in os.listdir("dbt_packages")
        lock1 = _read_lock()
        assert any("dbt_utils" in n for n in _lock_pkg_names(lock1))

        # Change unrelated env var (DBT_TEST_ENABLE_UTILS stays 'true')
        monkeypatch.setenv("DBT_UNRELATED_VAR", "bar")

        # Run 2: no re-lock, dbt_utils still installed
        _, stdout = run_dbt_and_capture(["deps"])
        assert "Updating lock file" not in stdout
        assert "dbt_utils" in os.listdir("dbt_packages")


# ---------------------------------------------------------------------------
# Transitive: unrelated var change does NOT trigger re-lock
# ---------------------------------------------------------------------------


class TestTransitiveNoRelockOnUnrelatedVarChange:
    """Transitive dep uses var('install_utils'). Changing a different var should not re-lock."""

    @pytest.fixture(scope="class")
    def models(self):
        return {"test_model.sql": model_sql}

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"vars": {"install_utils": True, "unrelated_var": "hello"}}

    @pytest.fixture(scope="class")
    def local_pkg(self, project):
        pkg_dir = os.path.join(project.project_root, "local_pkg")
        os.makedirs(pkg_dir, exist_ok=True)
        write_file(
            yaml.safe_dump({"name": "local_pkg", "version": "1.0.0"}),
            pkg_dir,
            "dbt_project.yml",
        )
        write_file(
            yaml.safe_dump(
                {
                    "packages": [
                        {
                            "package": "dbt-labs/dbt_utils",
                            "version": "1.3.0",
                            "enabled": "{{ var('install_utils') }}",
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
        return {"packages": [{"local": "local_pkg"}]}

    @pytest.fixture
    def clean_start(self, project):
        if os.path.exists("dbt_packages"):
            shutil.rmtree("dbt_packages")
        if os.path.exists("package-lock.yml"):
            os.remove("package-lock.yml")

    def test_unrelated_var_no_relock(self, project, local_pkg, clean_start):
        # Run 1: install_utils=true -> dbt_utils installed as transitive dep
        run_dbt(["deps"])
        installed = os.listdir("dbt_packages")
        assert "local_pkg" in installed
        assert "dbt_utils" in installed
        lock1 = _read_lock()
        assert "install_utils" in lock1["rendering_context"]["var_names"]

        # Change unrelated var in dbt_project.yml (install_utils stays true)
        write_file(
            yaml.safe_dump(
                {
                    "name": "test",
                    "profile": "test",
                    "flags": {"send_anonymous_usage_stats": False},
                    "vars": {"install_utils": True, "unrelated_var": "changed"},
                }
            ),
            project.project_root,
            "dbt_project.yml",
        )

        # Run 2: no re-lock, dbt_utils still installed
        _, stdout = run_dbt_and_capture(["deps"])
        assert "Updating lock file" not in stdout
        installed = os.listdir("dbt_packages")
        assert "local_pkg" in installed
        assert "dbt_utils" in installed


# ---------------------------------------------------------------------------
# Transitive: unrelated env_var change does NOT trigger re-lock
# ---------------------------------------------------------------------------


class TestTransitiveNoRelockOnUnrelatedEnvVarChange:
    """Transitive dep uses env_var('DBT_TEST_ENABLE_UTILS'). Changing a different env var should not re-lock."""

    @pytest.fixture(scope="class")
    def models(self):
        return {"test_model.sql": model_sql}

    @pytest.fixture(scope="class")
    def local_pkg(self, project):
        pkg_dir = os.path.join(project.project_root, "local_pkg")
        os.makedirs(pkg_dir, exist_ok=True)
        write_file(
            yaml.safe_dump({"name": "local_pkg", "version": "1.0.0"}),
            pkg_dir,
            "dbt_project.yml",
        )
        write_file(
            yaml.safe_dump(
                {
                    "packages": [
                        {
                            "package": "dbt-labs/dbt_utils",
                            "version": "1.3.0",
                            "enabled": "{{ env_var('DBT_TEST_ENABLE_UTILS', 'true') == 'true' }}",
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
        return {"packages": [{"local": "local_pkg"}]}

    @pytest.fixture
    def clean_start(self, project):
        if os.path.exists("dbt_packages"):
            shutil.rmtree("dbt_packages")
        if os.path.exists("package-lock.yml"):
            os.remove("package-lock.yml")

    def test_unrelated_env_var_no_relock(self, project, local_pkg, clean_start, monkeypatch):
        # Run 1: DBT_TEST_ENABLE_UTILS=true -> dbt_utils installed
        monkeypatch.setenv("DBT_TEST_ENABLE_UTILS", "true")
        monkeypatch.setenv("DBT_UNRELATED_VAR", "foo")
        run_dbt(["deps"])
        installed = os.listdir("dbt_packages")
        assert "local_pkg" in installed
        assert "dbt_utils" in installed
        lock1 = _read_lock()
        assert "DBT_TEST_ENABLE_UTILS" in lock1["rendering_context"]["env_var_names"]

        # Change unrelated env var (DBT_TEST_ENABLE_UTILS stays 'true')
        monkeypatch.setenv("DBT_UNRELATED_VAR", "bar")

        # Run 2: no re-lock, dbt_utils still installed
        _, stdout = run_dbt_and_capture(["deps"])
        assert "Updating lock file" not in stdout
        installed = os.listdir("dbt_packages")
        assert "local_pkg" in installed
        assert "dbt_utils" in installed
