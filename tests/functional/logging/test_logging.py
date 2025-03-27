import json
import os

import pytest

from dbt.events.types import InvalidOptionYAML
from dbt.tests.util import get_manifest, read_file, run_dbt
from dbt_common.events import EventLevel
from dbt_common.events.functions import fire_event

my_model_sql = """
  select 1 as fun
"""


@pytest.fixture(scope="class")
def models():
    return {"my_model.sql": my_model_sql}


# This test checks that various events contain node_info,
# which is supplied by the log_contextvars context manager
def test_basic(project, logs_dir):
    results = run_dbt(["--log-format=json", "run"])
    assert len(results) == 1
    manifest = get_manifest(project.project_root)
    assert "model.test.my_model" in manifest.nodes

    # get log file
    log_file = read_file(logs_dir, "dbt.log")
    assert log_file
    node_start = False
    node_finished = False
    connection_reused_data = []
    for log_line in log_file.split("\n"):
        # skip empty lines
        if len(log_line) == 0:
            continue
        # The adapter logging also shows up, so skip non-json lines
        if "[debug]" in log_line:
            continue
        log_dct = json.loads(log_line)
        log_data = log_dct["data"]
        log_event = log_dct["info"]["name"]
        if log_event == "ConnectionReused":
            connection_reused_data.append(log_data)
        if log_event == "NodeStart":
            node_start = True
        if log_event == "NodeFinished":
            node_finished = True
            assert log_data["run_result"]["adapter_response"]
        if node_start and not node_finished:
            if log_event == "NodeExecuting":
                assert "node_info" in log_data
            if log_event == "JinjaLogDebug":
                assert "node_info" in log_data
            if log_event == "SQLQuery":
                assert "node_info" in log_data
            if log_event == "TimingInfoCollected":
                assert "node_info" in log_data
                assert "timing_info" in log_data

    # windows doesn't have the same thread/connection flow so the ConnectionReused
    # events don't show up
    if os.name != "nt":
        # Verify the ConnectionReused event occurs and has the right data
        assert connection_reused_data
        for data in connection_reused_data:
            assert "conn_name" in data and data["conn_name"]
            assert "orig_conn_name" in data and data["orig_conn_name"]


def test_formatted_logs(project, logs_dir):
    # a basic run of dbt with a single model should have 5 `Formatting` events in the json logs
    results = run_dbt(["--log-format=json", "run"])
    assert len(results) == 1

    # get log file
    json_log_file = read_file(logs_dir, "dbt.log")
    formatted_json_lines = 0
    for log_line in json_log_file.split("\n"):
        # skip the empty line at the end
        if len(log_line) == 0:
            continue
        log_dct = json.loads(log_line)
        log_event = log_dct["info"]["name"]
        if log_event == "Formatting":
            formatted_json_lines += 1

    assert formatted_json_lines == 5


def test_invalid_event_value(project, logs_dir):
    results = run_dbt(["--log-format=json", "run"])
    assert len(results) == 1
    with pytest.raises(Exception):
        # This should raise because positional arguments are provided to the event
        fire_event(InvalidOptionYAML("testing"))

    # Provide invalid type to "option_name"
    with pytest.raises(Exception) as excinfo:
        fire_event(InvalidOptionYAML(option_name=1))

    assert "[InvalidOptionYAML]: Unable to parse logging event dictionary." in str(excinfo.value)


groups_yml = """
groups:
  - name: my_group_with_owner_metadata
    owner:
      name: my_name
      email: my.email@gmail.com
      slack: my_slack
      other_property: something_else

models:
  - name: my_model
    group: my_group_with_owner_metadata
    access: public
"""

groups_yml_with_multiple_emails = """
groups:
  - name: my_group_with_multiple_emails
    owner:
      name: my_name
      email:
        - my.email@gmail.com
        - my.second.email@gmail.com
      slack: my_slack
      other_property: something_else

models:
  - name: my_model
    group: my_group_with_multiple_emails
    access: public
    columns:
      - name: my_column
        tests:
         - not_null
"""


class TestRunResultErrorNodeInfo:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model.sql": "select not_found as id",
        }

    def test_node_info_on_results(self, project, logs_dir):
        results = run_dbt(["--log-format=json", "run"], expect_pass=False)
        assert len(results) == 1

        log_file = read_file(logs_dir, "dbt.log")

        for log_line in log_file.split("\n"):
            if not log_line:
                continue

            log_json = json.loads(log_line)
            if log_json["info"]["level"] == EventLevel.DEBUG:
                continue

            if log_json["info"]["name"] == "RunResultError":
                assert "node_info" in log_json["data"]
                assert log_json["data"]["node_info"]["unique_id"] == "model.test.my_model"
                assert "Database Error" in log_json["data"]["msg"]


def assert_group_data(group_data):
    assert group_data["name"] == "my_group_with_owner_metadata"
    assert group_data["owner"] == {
        "name": "my_name",
        "email": "my.email@gmail.com",
        "slack": "my_slack",
        "other_property": "something_else",
    }


class TestRunResultErrorGroup:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model.sql": "select not_found as id",
            "groups.yml": groups_yml,
        }

    def test_node_info_on_results(self, project, logs_dir):
        results = run_dbt(["--log-format=json", "run"], expect_pass=False)
        assert len(results) == 1

        log_file = read_file(logs_dir, "dbt.log")
        run_result_error_count = 0

        for log_line in log_file.split("\n"):
            if not log_line:
                continue

            log_json = json.loads(log_line)
            if log_json["info"]["level"] == EventLevel.DEBUG:
                continue

            if log_json["info"]["name"] == "RunResultError":
                assert "group" in log_json["data"]
                assert_group_data(log_json["data"]["group"])
                run_result_error_count += 1

        assert run_result_error_count == 1


class TestRunResultFailureGroup:
    @pytest.fixture(scope="class")
    def models(self):
        schema_yml = (
            groups_yml
            + """
    columns:
      - name: my_column
        tests:
         - not_null
"""
        )
        print(schema_yml)
        return {
            "my_model.sql": "select 1 as id, null as my_column",
            "groups.yml": schema_yml,
        }

    def test_node_info_on_results(self, project, logs_dir):
        results = run_dbt(["--log-format=json", "build"], expect_pass=False)
        assert len(results) == 2

        log_file = read_file(logs_dir, "dbt.log")
        run_result_error_count = 0
        run_result_failure_count = 0

        for log_line in log_file.split("\n"):
            if not log_line:
                continue

            log_json = json.loads(log_line)
            if log_json["info"]["level"] == EventLevel.DEBUG:
                continue

            if log_json["info"]["name"] == "RunResultError":
                assert "group" in log_json["data"]
                assert_group_data(log_json["data"]["group"])
                run_result_error_count += 1

            if log_json["info"]["name"] == "RunResultFailure":
                assert "group" in log_json["data"]
                assert_group_data(log_json["data"]["group"])
                run_result_failure_count += 1

        assert run_result_error_count == 1
        assert run_result_failure_count == 1


class TestRunResultWarningGroup:
    @pytest.fixture(scope="class")
    def models(self):
        schema_yml = (
            groups_yml
            + """
    columns:
      - name: my_column
        tests:
         - not_null:
             config:
               severity: warn
"""
        )
        print(schema_yml)
        return {
            "my_model.sql": "select 1 as id, null as my_column",
            "groups.yml": schema_yml,
        }

    def test_node_info_on_results(self, project, logs_dir):
        results = run_dbt(["--log-format=json", "build"])
        assert len(results) == 2

        log_file = read_file(logs_dir, "dbt.log")
        run_result_warning_count = 0

        for log_line in log_file.split("\n"):
            if not log_line:
                continue

            log_json = json.loads(log_line)
            if log_json["info"]["level"] == EventLevel.DEBUG:
                continue

            if log_json["info"]["name"] == "RunResultWarning":
                assert "group" in log_json["data"]
                assert_group_data(log_json["data"]["group"])
                run_result_warning_count += 1

        assert run_result_warning_count == 1


class TestRunResultNoGroup:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model.sql": "select 1 as id",
        }

    def test_node_info_on_results(self, project, logs_dir):
        results = run_dbt(["--no-write-json", "run"])
        assert len(results) == 1


class TestRunResultGroupWithMultipleEmails:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model.sql": "select 1 as id, null as my_column",
            "groups.yml": groups_yml_with_multiple_emails,
        }

    def test_node_info_on_results(self, project, logs_dir):
        results = run_dbt(["--log-format=json", "build"], expect_pass=False)
        assert len(results) == 2

        log_file = read_file(logs_dir, "dbt.log")
        run_result_error_count = 0

        for log_line in log_file.split("\n"):
            if not log_line:
                continue

            log_json = json.loads(log_line)
            if log_json["info"]["level"] == EventLevel.DEBUG:
                continue

            if log_json["info"]["name"] == "RunResultError":
                assert "group" in log_json["data"]
                group_data = log_json["data"]["group"]
                assert group_data["name"] == "my_group_with_multiple_emails"
                assert group_data["owner"] == {
                    "name": "my_name",
                    "email": "['my.email@gmail.com', 'my.second.email@gmail.com']",
                    "slack": "my_slack",
                    "other_property": "something_else",
                }
                run_result_error_count += 1

        assert run_result_error_count == 1
