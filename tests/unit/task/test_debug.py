"""Unit tests for dbt/task/debug.py.

Regression test for https://github.com/dbt-labs/dbt-core/issues/9436 —
_choose_profile_names() raised UnboundLocalError when raw_profile_data was
falsy (profiles.yml missing or unreadable).
"""
from unittest.mock import MagicMock, patch

import pytest

from dbt.task.debug import DebugTask


def _make_task(raw_profile_data=None, profile_path="/home/user/.dbt/profiles.yml"):
    """Create a minimal DebugTask with mocked flags."""
    args = MagicMock()
    args.profile = None
    args.VERSION_CHECK = False

    with patch("dbt.task.debug.DebugTask.__init__", return_value=None):
        task = DebugTask.__new__(DebugTask)

    task.args = args
    task.cli_vars = {}
    task.raw_profile_data = raw_profile_data
    task.profile_path = profile_path
    task.project_path = "/nonexistent/dbt_project.yml"
    return task


class TestChooseProfileNames:
    def test_no_raw_profile_data_returns_friendly_message(self):
        """When raw_profile_data is None, return a message instead of raising UnboundLocalError."""
        task = _make_task(raw_profile_data=None)
        profiles, message = task._choose_profile_names()
        assert profiles == []
        assert "profiles.yml" in message or task.profile_path in message

    def test_empty_raw_profile_data_returns_friendly_message(self):
        """When raw_profile_data is an empty dict, return a message without error."""
        task = _make_task(raw_profile_data={})
        profiles, message = task._choose_profile_names()
        assert profiles == []
        assert isinstance(message, str)
        assert len(message) > 0

    def test_raw_profile_data_with_no_profiles_returns_empty_profiles_message(self):
        """profiles.yml exists but has only a 'config' key — no usable profiles."""
        task = _make_task(raw_profile_data={"config": {"send_anonymous_usage_stats": False}})
        profiles, message = task._choose_profile_names()
        assert profiles == []
        assert "no profiles" in message.lower()

    def test_raw_profile_data_with_one_profile(self):
        """Single profile entry produces a populated list and a non-empty message."""
        task = _make_task(raw_profile_data={"default": {"outputs": {}}})
        profiles, message = task._choose_profile_names()
        assert "default" in profiles
        assert isinstance(message, str)
