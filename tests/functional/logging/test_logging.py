import pytest
from dbt.tests.util import run_dbt, get_manifest, read_file
import json


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
    for log_line in log_file.split('\n'):
        # skip empty lines
        if len(log_line) == 0:
            continue
        # The adapter logging also shows up, so skip non-json lines
        if "[debug]" in log_line:
            continue
        log_dct = json.loads(log_line)
        log_event = log_dct['info']['name']
        if log_event == "NodeStart":
            node_start = True
        if log_event == "NodeFinished":
            node_finished = True
        if node_start and not node_finished:
            if log_event == 'NodeExecuting':
                assert "node_info" in log_dct
            if log_event == "JinjaLogDebug":
                assert "node_info" in log_dct
            if log_event == "SQLQuery":
                assert "node_info" in log_dct
            if log_event == "TimingInfoCollected":
                assert "node_info" in log_dct
                assert "timing_info" in log_dct
