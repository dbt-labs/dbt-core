import os

import pytest

from dbt.tests.util import run_dbt

TEMP_ENV_VARS = {}
ENV_VARS_TO_SUSPEND = ["DBT_RECORDER_MODE"]


@pytest.fixture(scope="session", autouse=True)
def tests_setup_and_teardown():
    # Will be executed before the first test
    old_environ = dict(os.environ)
    os.environ.update(TEMP_ENV_VARS)
    for env_var in ENV_VARS_TO_SUSPEND:
        os.environ.pop(env_var, default=None)

    yield
    # Will be executed after the last test
    os.environ.clear()
    os.environ.update(old_environ)


@pytest.fixture(scope="session")
def set_dbt_recorder_mode():
    old_environ = os.environ
    os.environ["DBT_RECORDER_MODE"] = "record"
    yield
    os.environ = old_environ


class TestRecord:
    def test_record_when_env_var_set(self, project, set_dbt_recorder_mode):
        run_dbt(["run"])
        assert os.path.isfile(os.path.join(os.getcwd(), "recording.json"))
