import os
from unittest import mock

import pytest

from dbt.exceptions import DbtProjectError
from dbt.tests.util import run_dbt, update_config_file


class TestUserSettingsApplied:
    """user_settings.yml flags override code defaults."""

    def test_user_settings_override_default(self, project):
        update_config_file({"require-dbt-version": "0.0.0"}, "dbt_project.yml")

        # Default version_check is True, so parse should raise.
        with pytest.raises(DbtProjectError):
            run_dbt(["parse"])

        # User settings set version_check to false — parse should succeed.
        with mock.patch(
            "dbt.cli.flags.get_user_setting_flags",
            return_value={"version_check": False},
        ):
            run_dbt(["parse"])


class TestCLIOverridesUserSettings:
    """CLI flags take precedence over user_settings.yml."""

    def test_cli_overrides_user_settings(self, project):
        update_config_file({"require-dbt-version": "0.0.0"}, "dbt_project.yml")

        # User settings disable version_check, but CLI re-enables it.
        with mock.patch(
            "dbt.cli.flags.get_user_setting_flags",
            return_value={"version_check": False},
        ):
            with pytest.raises(DbtProjectError):
                run_dbt(["parse", "--version-check"])


class TestEnvVarOverridesUserSettings:
    """Environment variables take precedence over user_settings.yml."""

    def test_env_var_overrides_user_settings(self, project):
        update_config_file({"require-dbt-version": "0.0.0"}, "dbt_project.yml")

        # User settings disable version_check, but env var re-enables it.
        with mock.patch(
            "dbt.cli.flags.get_user_setting_flags",
            return_value={"version_check": False},
        ):
            with mock.patch.dict(os.environ, {"DBT_VERSION_CHECK": "true"}):
                with pytest.raises(DbtProjectError):
                    run_dbt(["parse"])


class TestProjectFlagsOverrideUserSettings:
    """Project flags in dbt_project.yml take precedence over user_settings.yml."""

    def test_project_flags_override_user_settings(self, project):
        update_config_file(
            {"require-dbt-version": "0.0.0", "flags": {"version_check": True}},
            "dbt_project.yml",
        )

        # User settings disable version_check, but project flags re-enable it.
        with mock.patch(
            "dbt.cli.flags.get_user_setting_flags",
            return_value={"version_check": False},
        ):
            with pytest.raises(DbtProjectError):
                run_dbt(["parse"])
