import os

import pytest

from dbt.tests.util import read_file, run_dbt

model_one_sql = """
    select 1 as fun
"""


class TestDeprecatedEnvVars:
    @pytest.fixture(scope="class")
    def models(self):
        return {"model_one.sql": model_one_sql}

    def test_defer(self, project, logs_dir):
        self.assert_deprecated(
            logs_dir,
            "DBT_DEFER_TO_STATE",
            "DBT_DEFER",
        )

    def test_favor_state(self, project, logs_dir):
        self.assert_deprecated(
            logs_dir,
            "DBT_FAVOR_STATE_MODE",
            "DBT_FAVOR_STATE",
            command="build",
        )

    def test_print(self, project, logs_dir):
        self.assert_deprecated(
            logs_dir,
            "DBT_NO_PRINT",
            "DBT_PRINT",
        )

    def test_state(self, project, logs_dir):
        self.assert_deprecated(
            logs_dir,
            "DBT_ARTIFACT_STATE_PATH",
            "DBT_STATE",
            old_val=".",
        )

    def assert_deprecated(self, logs_dir, old_env_var, new_env_var, command="run", old_val="0"):
        os.environ[old_env_var] = old_val
        run_dbt([command])

        # replacing new lines with spaces accounts for text wrapping
        log_file = read_file(logs_dir, "dbt.log").replace("\n", " ").replace("\\n", " ")
        dep_str = f"The environment variable `{old_env_var}` has been renamed as `{new_env_var}`"

        try:
            assert dep_str in log_file
        except Exception as e:
            del os.environ[old_env_var]
            raise e
        del os.environ[old_env_var]
