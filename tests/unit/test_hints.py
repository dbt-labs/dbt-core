import json
from pathlib import Path
from unittest import mock

import pytest

from dbt import hints
from dbt.hints import (
    HINT_PREFIX,
    HINT_TS_FILENAME,
    HintType,
    has_hint_cooldown,
    hint_to_msg_map,
    record_hint_shown,
    show_hint,
)


@pytest.fixture(autouse=True)
def dbt_home(monkeypatch, tmp_path):
    # The hint file lives in the dbt home dir (~/.dbt); point HOME at a temp dir
    # so tests read/write there instead of the real home. Returns the ~/.dbt dir.
    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.setenv("USERPROFILE", str(tmp_path))
    dbt_dir = tmp_path / ".dbt"
    dbt_dir.mkdir()
    return dbt_dir


def test_hint_to_msg_map_covers_every_hint_type():
    # Every HintType must have a message, otherwise show_hint would KeyError.
    assert set(hint_to_msg_map) == set(HintType)


class TestHintCooldown:
    def test_no_cooldown_when_file_missing(self):
        assert has_hint_cooldown(HintType.LONG_PARSING_WITHOUT_V2_PARSER) is False

    def test_recent_hint_is_on_cooldown(self):
        record_hint_shown(HintType.LONG_PARSING_WITHOUT_V2_PARSER)
        assert has_hint_cooldown(HintType.LONG_PARSING_WITHOUT_V2_PARSER) is True

    def test_expired_hint_is_not_on_cooldown(self, dbt_home):
        stale = {HintType.LONG_PARSING_WITHOUT_V2_PARSER: 0}  # epoch => long ago
        (dbt_home / HINT_TS_FILENAME).write_text(json.dumps(stale))
        assert has_hint_cooldown(HintType.LONG_PARSING_WITHOUT_V2_PARSER) is False

    def test_cooldown_tolerates_corrupt_file(self, dbt_home):
        (dbt_home / HINT_TS_FILENAME).write_text("{not valid json")
        assert has_hint_cooldown(HintType.LONG_PARSING_WITHOUT_V2_PARSER) is False

    def test_cooldown_tolerates_unreadable_file(self, mocker):
        # A read failure (e.g. permission denied) must not raise, just show hints.
        mocker.patch.object(Path, "read_text", side_effect=PermissionError("denied"))
        assert has_hint_cooldown(HintType.LONG_PARSING_WITHOUT_V2_PARSER) is False

    def test_record_persists_to_disk(self, dbt_home):
        record_hint_shown(HintType.REUSE_RELATIONS_ON_TOO_MANY_MODELS)

        on_disk = json.loads((dbt_home / HINT_TS_FILENAME).read_text())
        assert HintType.REUSE_RELATIONS_ON_TOO_MANY_MODELS in on_disk
        stored = on_disk[HintType.REUSE_RELATIONS_ON_TOO_MANY_MODELS]
        # Persisted as an int (epoch seconds) for cross-compat with the Rust engine.
        assert isinstance(stored, int)
        assert stored > 0

    def test_record_creates_home_dir_if_missing(self, tmp_path, monkeypatch):
        # ~/.dbt may not exist yet; record_hint_shown should create it.
        home = tmp_path / "fresh"
        monkeypatch.setenv("HOME", str(home))
        monkeypatch.setenv("USERPROFILE", str(home))

        record_hint_shown(HintType.REUSE_RELATIONS_ON_TOO_MANY_MODELS)
        assert (home / ".dbt" / HINT_TS_FILENAME).exists()

    def test_record_swallows_write_errors(self, mocker):
        # A failed write (read-only dir, disk full, ...) must not raise.
        mocker.patch.object(Path, "write_text", side_effect=OSError("read-only"))
        record_hint_shown(HintType.REUSE_RELATIONS_ON_TOO_MANY_MODELS)


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
        assert note.msg == HINT_PREFIX + hint_to_msg_map[HintType.LONG_PARSING_WITHOUT_V2_PARSER]
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

    def test_skips_when_within_cooldown(self, mock_fire_event, mock_track_hint_view):
        record_hint_shown(HintType.REUSE_RELATIONS_ON_TOO_MANY_MODELS)
        with self._mock_flags(hints_enabled=True):
            show_hint(HintType.REUSE_RELATIONS_ON_TOO_MANY_MODELS)

        mock_fire_event.assert_not_called()
        mock_track_hint_view.assert_not_called()

    def test_shows_and_records_when_cooldown_expired(
        self, dbt_home, mock_fire_event, mock_track_hint_view
    ):
        # A stale timestamp so the hint is eligible again.
        stale = {HintType.REUSE_RELATIONS_ON_TOO_MANY_MODELS: 0}
        (dbt_home / HINT_TS_FILENAME).write_text(json.dumps(stale))

        with self._mock_flags(hints_enabled=True):
            show_hint(HintType.REUSE_RELATIONS_ON_TOO_MANY_MODELS)

        mock_fire_event.assert_called_once()
        mock_track_hint_view.assert_called_once()
        # The shown hint is now on cooldown again.
        assert has_hint_cooldown(HintType.REUSE_RELATIONS_ON_TOO_MANY_MODELS) is True
