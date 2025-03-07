import json

import pytest

from dbt.tests.util import read_file, run_dbt

model1 = "select 1 as fun"
model2 = '{{ config(meta={"owners": ["team1", "team2"]})}} select 1 as fun'
model3 = '{{ config(meta={"key": 1})}} select 1 as fun'


@pytest.fixture(scope="class")  # noqa
def models():
    return {"model1.sql": model1, "model2.sql": model2, "model3.sql": model3}


def run_and_capture_node_info_logs(logs_dir):
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

        yield log_dct["data"]["node_info"]


# This test checks that various events contain node_info,
# which is supplied by the log_contextvars context manager
def test_meta(project, logs_dir):
    for node_info_log in run_and_capture_node_info_logs(logs_dir):
        if node_info_log["unique_id"] == "model.test.model1":
            assert node_info_log["meta"] == {}
        elif node_info_log["unique_id"] == "model.test.model2":
            assert node_info_log["meta"] == {"owners": ["team1", "team2"]}
        elif node_info_log["unique_id"] == "model.test.model3":
            assert node_info_log["meta"] == {"key": 1}


def test_checksum(project, logs_dir):
    for node_info_log in run_and_capture_node_info_logs(logs_dir):
        if node_info_log["unique_id"] == "model.test.model1":
            assert (
                node_info_log["node_checksum"]
                == "7a72de8ca68190cc1f3a600b99ad24ce701817a5674222778845eb939c64aa76"
            )
        elif node_info_log["unique_id"] == "model.test.model2":
            assert (
                node_info_log["node_checksum"]
                == "4e5b7658359b9a7fec6aa3cbad98ab07725927ccce59ec6e511e599e000b0fd3"
            )
        elif node_info_log["unique_id"] == "model.test.model3":
            assert (
                node_info_log["node_checksum"]
                == "99c67d153920066d43168cc495240f185cec9d8cd552e7778e08437e66f44da7"
            )
