import pytest
from dbt.tests.util import run_dbt, get_manifest, read_file
import json
import os
from dbt.events.functions import fire_event
from dbt.events.types import InvalidOptionYAML


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

    assert str(excinfo.value) == "[InvalidOptionYAML]: Unable to parse dict {'option_name': 1}"
