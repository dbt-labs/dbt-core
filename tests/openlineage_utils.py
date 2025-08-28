import re
from enum import Enum
from typing import Dict

import attr
import pytest
from google.protobuf.json_format import ParseDict
from openlineage.client.event_v2 import RunEvent

from dbt.events import ALL_PROTO_TYPES
from dbt_common.events.base_types import EventMsg


def ol_event_to_dict(event: RunEvent) -> Dict:
    def serialize(inst, field, value):
        if isinstance(value, Enum):
            return value.value
        return value

    return attr.asdict(event, value_serializer=serialize)


def dict_to_event_msg(e: Dict) -> EventMsg:
    msg_class_name = e["info"]["name"] + "Msg"
    msg_cls = ALL_PROTO_TYPES[msg_class_name]
    return ParseDict(e, msg_cls())


def assert_ol_events_match(expected_event, actual_event):
    """
    Match two openlineage events.
    Only check  for keys that are present in expected_event.
    When there is a mismatch, raise an AssertionError with the json path to the mismatch.
    """

    def assert_ol_event_match_rec(expected_event, actual_event, path_to_key):
        # dict case
        if isinstance(expected_event, dict):
            for key in expected_event:
                if key not in actual_event:
                    path_to_key_str = ".".join(path_to_key + [key])
                    raise AssertionError(f"Key {path_to_key_str} not found in actual event")
                else:
                    assert_ol_event_match_rec(
                        expected_event[key], actual_event[key], path_to_key + [key]
                    )
        # list case
        elif isinstance(expected_event, list):
            if len(expected_event) != len(actual_event):
                path_to_key_str = ".".join(path_to_key + ["[*]"])
                raise AssertionError(
                    f"List length mismatch at path {path_to_key_str}, expected {len(expected_event)}, actual {len(actual_event)}"
                )
            else:
                for i, (expected_item, actual_item) in enumerate(
                    zip(expected_event, actual_event)
                ):
                    assert_ol_event_match_rec(expected_item, actual_item, path_to_key + [f"[{i}]"])

        # literal string case with regex
        elif isinstance(expected_event, str) and _is_regex(expected_event):

            if not _is_match(expected_event, actual_event):
                path_to_key = ".".join(path_to_key)
                assert (
                    expected_event == actual_event
                ), f'Value mismatch at path="{path_to_key}"\n It doesn\'t follow expected regex. Checkout value at path in actual event.'
        # other literal cases
        else:
            path_to_key = ".".join(path_to_key)
            assert (
                expected_event == actual_event
            ), f'Value mismatch at path="{path_to_key}", Checkout value at path in actual event.'

    path_to_key = ["root"]
    assert_ol_event_match_rec(expected_event, actual_event, path_to_key)


def _is_regex(string) -> bool:
    """
    True if the string literal contains a regex.
    This is used in tests
    foo_{{ abc }}_d will match foo_abc_d
    """

    TEST_REGEX = r"\{\{ .* \}\}"
    return re.search(TEST_REGEX, string) is not None


def _is_match(pattern, str) -> bool:
    escaped_pattern = _escape_special_regex_chars(pattern)
    return re.match(escaped_pattern, str) is not None


def _escape_special_regex_chars(s) -> str:
    """
    Escape all chars that are not surrounded by double {{ }}
    """

    def _append_string_chunks(escape=True):
        nonlocal string_chunk, escaped_string
        if string_chunk:
            string_chunk_str = "".join(string_chunk)
            if escape:
                string_chunk_str = re.escape(string_chunk_str)
            escaped_string.append(string_chunk_str)
            string_chunk = []

    i = 0
    escaped_string = []
    string_chunk = []
    while i < len(s):
        if s[i : i + 3] == "{{ ":
            _append_string_chunks()
            i += 3
            while i < len(s) and s[i : i + 3] != " }}":
                string_chunk.append(s[i])
                i += 1
            _append_string_chunks(escape=False)
            i += 3
        else:
            string_chunk.append(s[i])
            i += 1
    _append_string_chunks()
    return "".join(escaped_string)


@pytest.fixture(scope="function")
def openlineage_handler_with_dummy_emit(monkeypatch):
    """
    Saves the events emitted by Openlineage handler.
    """

    class DummyOpenLineageHandler:
        def __init__(self):
            self.emitted_events = []

        def dummy_emit(self, x: RunEvent):
            self.emitted_events.append(x)

    dummy_ol_handler = DummyOpenLineageHandler()
    monkeypatch.setattr(
        "dbt.openlineage.handler.OpenLineageHandler.emit", dummy_ol_handler.dummy_emit
    )

    yield dummy_ol_handler


@pytest.fixture(scope="function")
def openlineage_handler_with_raise_exception(monkeypatch):
    """
    In integration tests we want the tests to fail when there is an exception
    """

    def raise_exception(self, exception: Exception):
        raise exception

    monkeypatch.setattr(
        "dbt.openlineage.handler.OpenLineageHandler._handle_exception", raise_exception
    )
