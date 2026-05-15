import os
from unittest import mock

import pytest

from dbt.exceptions import DbtProjectError
from dbt.tests.util import run_dbt, update_config_file


class TestClickCLIFlagsResolveTruthy:
    def test_resolve_truthy(self, project):
        # we can't do this in a fixture because the project will error out before the test can run
        update_config_file({"require-dbt-version": "0.0.0"}, "dbt_project.yml")
        with pytest.raises(DbtProjectError):
            run_dbt(["parse", "--version-check"])


class TestClickEnvVarFlagsResolveTruthy:
    @pytest.mark.parametrize("env_var_value", ["yes", "y", "true", "t", "on", "1"])
    def test_resolve_truthy(self, project, env_var_value: str):
        # we can't do this in a fixture because the project will error out before the test can run
        update_config_file({"require-dbt-version": "0.0.0"}, "dbt_project.yml")

        with mock.patch.dict(os.environ, {"DBT_VERSION_CHECK": env_var_value}):
            # should raise
            with pytest.raises(DbtProjectError):
                run_dbt(["parse"])


class TestClickCLIFlagsResolveFalsey:
    def test_resolve_falsey(self, project):
        # we can't do this in a fixture because the project will error out before the test can run
        update_config_file({"require-dbt-version": "0.0.0"}, "dbt_project.yml")

        # shouldn't raise
        run_dbt(["parse", "--no-version-check"])


class TestClickEnvVarFlagsResolveFalsey:
    @pytest.mark.parametrize("env_var_value", ["no", "n", "false", "f", "off", "0"])
    def test_resolve_falsey(self, project, env_var_value: str):
        # we can't do this in a fixture because the project will error out before the test can run
        update_config_file({"require-dbt-version": "0.0.0"}, "dbt_project.yml")

        with mock.patch.dict(os.environ, {"DBT_VERSION_CHECK": env_var_value}):
            # shouldn't raise
            run_dbt(["parse"])
