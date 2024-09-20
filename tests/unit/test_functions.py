from argparse import Namespace

import pytest

import dbt.flags as flags
from dbt.adapters.events.types import AdapterDeprecationWarning
from dbt.events.types import NoNodesForSelectionCriteria
from dbt_common.events.functions import msg_to_dict, warn_or_error
from dbt_common.events.types import InfoLevel, RetryExternalCall
from dbt_common.exceptions import EventCompilationError


@pytest.mark.parametrize(
    "warn_error_options,expect_compilation_exception",
    [
        ({"include": "all"}, True),
        ({"include": ["NoNodesForSelectionCriteria"]}, True),
        ({"include": []}, False),
        ({}, False),
        ({"include": ["MainTrackingUserState"]}, False),
        ({"include": "all", "exclude": ["NoNodesForSelectionCriteria"]}, False),
    ],
)
def test_warn_or_error_warn_error_options(warn_error_options, expect_compilation_exception):
    args = Namespace(warn_error_options=warn_error_options)
    flags.set_from_args(args, {})
    if expect_compilation_exception:
        with pytest.raises(EventCompilationError):
            warn_or_error(NoNodesForSelectionCriteria())
    else:
        warn_or_error(NoNodesForSelectionCriteria())


@pytest.mark.parametrize(
    "error_cls",
    [
        NoNodesForSelectionCriteria,  # core event
        AdapterDeprecationWarning,  # adapter event
        RetryExternalCall,  # common event
    ],
)
def test_warn_error_options_captures_all_events(error_cls):
    args = Namespace(warn_error_options={"include": [error_cls.__name__]})
    flags.set_from_args(args, {})
    with pytest.raises(EventCompilationError):
        warn_or_error(error_cls())

    args = Namespace(warn_error_options={"include": "*", "exclude": [error_cls.__name__]})
    flags.set_from_args(args, {})
    warn_or_error(error_cls())


@pytest.mark.parametrize(
    "warn_error,expect_compilation_exception",
    [
        (True, True),
        (False, False),
    ],
)
def test_warn_or_error_warn_error(warn_error, expect_compilation_exception):
    args = Namespace(warn_error=warn_error)
    flags.set_from_args(args, {})
    if expect_compilation_exception:
        with pytest.raises(EventCompilationError):
            warn_or_error(NoNodesForSelectionCriteria())
    else:
        warn_or_error(NoNodesForSelectionCriteria())


def test_msg_to_dict_handles_exceptions_gracefully():
    class BadEvent(InfoLevel):
        """A spoof Note event which breaks dictification"""

        def __init__(self):
            self.__class__.__name__ = "Note"

    event = BadEvent()
    try:
        msg_to_dict(event)
    except Exception as exc:
        assert (
            False
        ), f"We expect `msg_to_dict` to gracefully handle exceptions, but it raised {exc}"
