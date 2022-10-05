import pytest
import json
import os

from dbt.constants import METADATA_ENV_PREFIX
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
        os.environ[METADATA_ENV_PREFIX + "some_var"] = "value"
        yield
        del os.environ[METADATA_ENV_PREFIX + "some_var"]

    def test_extra_filled(self, project):
        _, log_output = run_dbt_and_capture(['--log-format=json', 'deps'],)
        logs = parse_json_logs(log_output)
        for log in logs:
            assert log['info'].get('extra') == {"some_var": "value"}
