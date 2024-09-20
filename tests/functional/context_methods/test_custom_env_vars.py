import json
import os

import pytest

from dbt.tests.util import run_dbt_and_capture


def parse_json_logs(json_log_output):
    parsed_logs = []
    for line in json_log_output.split("\n"):
        try:
            log = json.loads(line)
        except ValueError:
            continue

        parsed_logs.append(log)

    return parsed_logs


class TestCustomVarInLogs:
    @pytest.fixture(scope="class", autouse=True)
    def setup(self):
        # on windows, python uppercases env var names because windows is case insensitive
        os.environ["DBT_ENV_CUSTOM_ENV_SOME_VAR"] = "value"
        yield
        del os.environ["DBT_ENV_CUSTOM_ENV_SOME_VAR"]

    def test_extra_filled(self, project):
        _, log_output = run_dbt_and_capture(
            ["--log-format=json", "deps"],
        )
        logs = parse_json_logs(log_output)
        for log in logs:
            assert log["info"].get("extra") == {"SOME_VAR": "value"}
