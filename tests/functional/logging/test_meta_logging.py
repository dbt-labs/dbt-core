import pytest
from dbt.tests.util import run_dbt, read_file
import json

model1 = "select 1 as fun"
model2 = '{{ config(meta={"owners": ["team1", "team2"]})}} select 1 as fun'
model3 = '{{ config(meta={"key": 1})}} select 1 as fun'


@pytest.fixture(scope="class")  # noqa
def models():
    return {"model1.sql": model1, "model2.sql": model2, "model3.sql": model3}


# This test checks that various events contain node_info,
# which is supplied by the log_contextvars context manager
def test_meta(project, logs_dir):
    run_dbt(["--log-format=json", "run"])

    # get log file
    log_file = read_file(logs_dir, "dbt.log")
    assert log_file

    for log_line in log_file.split("\n"):
        # skip empty lines
        if len(log_line) == 0:
            continue
        # The adapter logging also shows up, so skip non-json lines
        if "[debug]" in log_line:
            continue

        log_dct = json.loads(log_line)
        if "node_info" not in log_dct["data"]:
            continue

        print(f"--- log_dct: {log_dct}")
        node_info = log_dct["data"]["node_info"]
        node_path = node_info["node_path"]
        if node_path == "model1.sql":
            assert node_info["meta"] == {}
        elif node_path == "model2.sql":
            assert node_info["meta"] == {"owners": ["team1", "team2"]}
        elif node_path == "model3.sql":
            assert node_info["meta"] == {"key": 1}
