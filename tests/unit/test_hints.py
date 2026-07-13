from unittest import mock

import pytest

from dbt import hints
from dbt.hints import HintType, hint_to_msg_map, show_hint


def test_hint_to_msg_map_covers_every_hint_type():
    # Every HintType member must have a message, otherwise show_hint would KeyError.
    assert set(hint_to_msg_map) == set(HintType)


class TestShowHint:
    @pytest.fixture
    def mock_fire_event(self):
        with mock.patch.object(hints, "fire_event") as m:
            yield m

    @pytest.fixture
    def mock_track_hint_view(self):
        with mock.patch.object(hints, "track_hint_view") as m:
            yield m

    def _mock_flags(self, hints_enabled: bool):
        return mock.patch.object(
            hints, "get_flags", return_value=mock.Mock(HINTS_ENABLED=hints_enabled)
        )

    def test_fires_event_and_telemetry_when_enabled(self, mock_fire_event, mock_track_hint_view):
        with self._mock_flags(hints_enabled=True):
            show_hint(HintType.LONG_PARSING_WITHOUT_V2_PARSER)

        mock_fire_event.assert_called_once()
        note = mock_fire_event.call_args.args[0]
        assert note.msg == hint_to_msg_map[HintType.LONG_PARSING_WITHOUT_V2_PARSER]
        mock_track_hint_view.assert_called_once_with(HintType.LONG_PARSING_WITHOUT_V2_PARSER)

    def test_does_nothing_when_disabled(self, mock_fire_event, mock_track_hint_view):
        with self._mock_flags(hints_enabled=False):
            show_hint(HintType.REUSE_RELATIONS_ON_TOO_MANY_MODELS)

        mock_fire_event.assert_not_called()
        mock_track_hint_view.assert_not_called()

    def test_defaults_to_enabled_when_flag_missing(self, mock_fire_event, mock_track_hint_view):
        # If HINTS_ENABLED isn't set on the flags object at all, hints still show.
        flags = mock.Mock(spec=[])  # no HINTS_ENABLED attribute
        with mock.patch.object(hints, "get_flags", return_value=flags):
            show_hint(HintType.REUSE_RELATIONS_ON_TOO_MANY_MODELS)

        mock_fire_event.assert_called_once()
        mock_track_hint_view.assert_called_once()
