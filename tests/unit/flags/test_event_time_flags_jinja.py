"""Tests for #12847: expose CLI event-time bounds on the Jinja flags object."""

from __future__ import annotations

from argparse import Namespace
from datetime import datetime, timezone

import dbt.flags as flags_mod
from dbt.flags import get_flag_obj


def test_event_time_exposed_when_set_on_global_flags():
    """--event-time-start/end on CLI should be readable as flags.EVENT_TIME_* in Jinja."""
    start = datetime(2024, 6, 1, tzinfo=timezone.utc)
    end = datetime(2024, 7, 1, tzinfo=timezone.utc)
    flags_mod.GLOBAL_FLAGS = Namespace(
        EVENT_TIME_START=start,
        EVENT_TIME_END=end,
        FULL_REFRESH=False,
    )

    flags = get_flag_obj()

    assert flags.EVENT_TIME_START == start
    assert flags.EVENT_TIME_END == end


def test_event_time_none_when_unset_on_global_flags():
    flags_mod.GLOBAL_FLAGS = Namespace(FULL_REFRESH=False)

    flags = get_flag_obj()

    assert flags.EVENT_TIME_START is None
    assert flags.EVENT_TIME_END is None
