from dbt.events.types import (
    MainReportVersion,
    MainReportArgs,
    RollbackFailed,
    MainEncounteredError,
    PluginLoadError,
    LogStartLine,
    LogTestResult,
)
from dbt.events.functions import msg_to_dict, msg_to_json, LOG_VERSION, reset_metadata_vars
from dbt.events import types_pb2
from dbt.events.base_types import msg_from_base_event, EventLevel
from dbt.version import installed
from google.protobuf.json_format import MessageToDict
from dbt.flags import set_from_args
from argparse import Namespace

set_from_args(Namespace(WARN_ERROR=False), None)


info_keys = {
    "name",
    "code",
    "msg",
    "level",
    "invocation_id",
    "pid",
    "thread",
    "ts",
    "extra",
    "category",
}


def test_events():

    # A001 event
    event = MainReportVersion(version=str(installed), log_version=LOG_VERSION)
    msg = msg_from_base_event(event)
    msg_dict = msg_to_dict(msg)
    msg_json = msg_to_json(msg)
    serialized = msg.SerializeToString()
    assert "Running with dbt=" in str(serialized)
    assert set(msg_dict.keys()) == {"info", "data"}
    assert set(msg_dict["data"].keys()) == {"version", "log_version"}
    assert set(msg_dict["info"].keys()) == info_keys
    assert msg_json
    assert msg.info.code == "A001"

    # Extract EventInfo from serialized message
    generic_msg = types_pb2.GenericMessage()
    generic_msg.ParseFromString(serialized)
    assert generic_msg.info.code == "A001"
    # get the message class for the real message from the generic message
    message_class = getattr(types_pb2, f"{generic_msg.info.name}Msg")
    new_msg = message_class()
    new_msg.ParseFromString(serialized)
    assert new_msg.info.code == msg.info.code
    assert new_msg.data.version == msg.data.version

    # A002 event
    event = MainReportArgs(args={"one": "1", "two": "2"})
    msg = msg_from_base_event(event)
    msg_dict = msg_to_dict(msg)
    msg_json = msg_to_json(msg)

    assert set(msg_dict.keys()) == {"info", "data"}
    assert set(msg_dict["data"].keys()) == {"args"}
    assert set(msg_dict["info"].keys()) == info_keys
    assert msg_json
    assert msg.info.code == "A002"


def test_exception_events():
    event = RollbackFailed(conn_name="test", exc_info="something failed")
    msg = msg_from_base_event(event)
    msg_dict = msg_to_dict(msg)
    msg_json = msg_to_json(msg)
    assert set(msg_dict.keys()) == {"info", "data"}
    assert set(msg_dict["data"].keys()) == {"conn_name", "exc_info"}
    assert set(msg_dict["info"].keys()) == info_keys
    assert msg_json
    assert msg.info.code == "E009"

    event = PluginLoadError(exc_info="something failed")
    msg = msg_from_base_event(event)
    msg_dict = msg_to_dict(msg)
    msg_json = msg_to_json(msg)
    assert set(msg_dict["data"].keys()) == {"exc_info"}
    assert set(msg_dict["info"].keys()) == info_keys
    assert msg_json
    assert msg.info.code == "E036"
    assert msg.info.msg == "something failed"

    # Z002 event
    event = MainEncounteredError(exc="Rollback failed")
    msg = msg_from_base_event(event)
    msg_dict = msg_to_dict(msg)
    msg_json = msg_to_json(msg)

    assert set(msg_dict["data"].keys()) == {"exc"}
    assert set(msg_dict["info"].keys()) == info_keys
    assert msg_json
    assert msg.info.code == "Z002"


def test_node_info_events():
    meta_dict = {
        "key1": ["value1", 2],
        "key2": {"nested-dict-key": "value2"},
        "key3": 1,
        "key4": ["string1", 1, "string2", 2],
    }
    node_info = {
        "node_path": "some_path",
        "node_name": "some_name",
        "unique_id": "some_id",
        "resource_type": "model",
        "materialized": "table",
        "node_status": "started",
        "node_started_at": "some_time",
        "node_finished_at": "another_time",
        "meta": meta_dict,
    }
    event = LogStartLine(
        description="some description",
        index=123,
        total=111,
        node_info=node_info,
    )
    assert event
    assert event.node_info.node_path == "some_path"
    assert event.to_dict()["node_info"]["meta"] == meta_dict


def test_extra_dict_on_event(monkeypatch):

    monkeypatch.setenv("DBT_ENV_CUSTOM_ENV_env_key", "env_value")

    reset_metadata_vars()

    event = MainReportVersion(version=str(installed), log_version=LOG_VERSION)
    msg = msg_from_base_event(event)
    msg_dict = msg_to_dict(msg)
    assert set(msg_dict["info"].keys()) == info_keys
    extra_dict = {"env_key": "env_value"}
    assert msg.info.extra == extra_dict
    serialized = msg.SerializeToString()

    # Extract EventInfo from serialized message
    generic_msg = types_pb2.GenericMessage()
    generic_msg.ParseFromString(serialized)
    assert generic_msg.info.code == "A001"
    # get the message class for the real message from the generic message
    message_class = getattr(types_pb2, f"{generic_msg.info.name}Msg")
    new_msg = message_class()
    new_msg.ParseFromString(serialized)
    new_msg_dict = MessageToDict(new_msg)
    assert new_msg_dict["info"]["extra"] == msg.info.extra

    # clean up
    reset_metadata_vars()


def test_dynamic_level_events():
    event = LogTestResult(name="model_name", status="pass", index=1, num_models=3, num_failures=0)
    msg = msg_from_base_event(event, level=EventLevel.INFO)
    assert msg
    assert msg.info.level == "info"
