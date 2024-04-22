import os
import pytest

from dbt.tests.util import run_dbt_and_capture
from dbt_common.constants import SECRET_ENV_PREFIX


class TestAllowSecretProfilePackage:
    @pytest.fixture(scope="class", autouse=True)
    def setUp(self):
        os.environ[SECRET_ENV_PREFIX + "FOR_LOGGING"] = "super secret"
        yield
        del os.environ[SECRET_ENV_PREFIX + "FOR_LOGGING"]

    @pytest.fixture(scope="class")
    def packages(self):
        return {
            "packages": [
                {
                    "package": "dbt-labs/dbt_utils{{ log(env_var('DBT_ENV_SECRET_FOR_LOGGING'), info = true) }}",
                    "version": "1.0.0",
                }
            ]
        }

    def test_allow_secrets(self, project):
        _, log_output = run_dbt_and_capture(["deps"])
        # this will not be written to logs or lock file
        assert not ("super secret" in log_output)
        assert "*****" in log_output
        assert not ("DBT_ENV_SECRET_FOR_LOGGING" in log_output)
