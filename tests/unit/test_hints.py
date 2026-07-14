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
    load_hint_ts,
    record_hint_shown,
    reset_hint_ts,
    show_hint,
)


@pytest.fixture(autouse=True)
def fresh_hint_ts():
    # The cooldown state is module-global; keep tests isolated from each other.
    reset_hint_ts()
    yield
    reset_hint_ts()


def test_hint_to_msg_map_covers_every_hint_type():
    # Every HintType must have a message, otherwise show_hint would KeyError.
    assert set(hint_to_msg_map) == set(HintType)


class TestHintCooldown:
    def test_load_is_empty_when_file_missing(self, tmp_path):
        assert load_hint_ts(tmp_path) == {}

    def test_load_reads_existing_file(self, tmp_path):
        (tmp_path / HINT_TS_FILENAME).write_text(json.dumps({"some_hint": 123.0}))
        assert load_hint_ts(tmp_path) == {"some_hint": 123.0}

    def test_same_path_is_cached_not_reread(self, tmp_path):
        hint_file = tmp_path / HINT_TS_FILENAME
        hint_file.write_text(json.dumps({"some_hint": 1.0}))
        assert load_hint_ts(tmp_path) == {"some_hint": 1.0}

        # Overwrite on disk; a re-load of the same path returns the cached copy.
        hint_file.write_text(json.dumps({"some_hint": 999.0}))
        assert load_hint_ts(tmp_path) == {"some_hint": 1.0}

    def test_different_paths_are_cached_separately(self, tmp_path):
        (tmp_path / HINT_TS_FILENAME).write_text(json.dumps({"a": 1.0}))
        other = tmp_path / "other"
        other.mkdir()
        (other / HINT_TS_FILENAME).write_text(json.dumps({"b": 2.0}))

        assert load_hint_ts(tmp_path) == {"a": 1.0}
        assert load_hint_ts(other) == {"b": 2.0}

    def test_load_tolerates_corrupt_file(self, tmp_path):
        (tmp_path / HINT_TS_FILENAME).write_text("{not valid json")
        assert load_hint_ts(tmp_path) == {}

    def test_no_cooldown_before_load(self):
        # Never loaded -> nothing is on cooldown, so hints are free to show.
        assert has_hint_cooldown(HintType.LONG_PARSING_WITHOUT_V2_PARSER) is False

    def test_recent_hint_is_on_cooldown(self, tmp_path):
        load_hint_ts(tmp_path)
        record_hint_shown(HintType.LONG_PARSING_WITHOUT_V2_PARSER)
        assert has_hint_cooldown(HintType.LONG_PARSING_WITHOUT_V2_PARSER) is True

    def test_expired_hint_is_not_on_cooldown(self, tmp_path):
        stale = {HintType.LONG_PARSING_WITHOUT_V2_PARSER: 0.0}  # epoch => long ago
        (tmp_path / HINT_TS_FILENAME).write_text(json.dumps(stale))
        load_hint_ts(tmp_path)
        assert has_hint_cooldown(HintType.LONG_PARSING_WITHOUT_V2_PARSER) is False

    def test_record_persists_to_disk(self, tmp_path):
        load_hint_ts(tmp_path)
        record_hint_shown(HintType.REUSE_RELATIONS_ON_TOO_MANY_MODELS)

        on_disk = json.loads((tmp_path / HINT_TS_FILENAME).read_text())
        assert HintType.REUSE_RELATIONS_ON_TOO_MANY_MODELS in on_disk
        stored = on_disk[HintType.REUSE_RELATIONS_ON_TOO_MANY_MODELS]
        # Persisted as an int (epoch seconds) for cross-compat with the Rust engine.
        assert isinstance(stored, int)
        assert stored > 0

    def test_record_is_noop_without_target(self):
        # Nothing loaded -> nowhere to write, and it must not raise.
        record_hint_shown(HintType.REUSE_RELATIONS_ON_TOO_MANY_MODELS)
        assert has_hint_cooldown(HintType.REUSE_RELATIONS_ON_TOO_MANY_MODELS) is False

    def test_record_swallows_write_errors(self, tmp_path, mocker):
        # A failed write (read-only dir, disk full, ...) must not raise.
        load_hint_ts(tmp_path)
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

    def test_skips_when_within_cooldown(self, tmp_path, mock_fire_event, mock_track_hint_view):
        load_hint_ts(tmp_path)
        record_hint_shown(HintType.REUSE_RELATIONS_ON_TOO_MANY_MODELS)
        with self._mock_flags(hints_enabled=True):
            show_hint(HintType.REUSE_RELATIONS_ON_TOO_MANY_MODELS)

        mock_fire_event.assert_not_called()
        mock_track_hint_view.assert_not_called()

    def test_shows_and_records_when_cooldown_expired(
        self, tmp_path, mock_fire_event, mock_track_hint_view
    ):
        # Load a stale timestamp so the hint is eligible again.
        stale = {HintType.REUSE_RELATIONS_ON_TOO_MANY_MODELS: 0.0}
        (tmp_path / HINT_TS_FILENAME).write_text(json.dumps(stale))
        load_hint_ts(tmp_path)

        with self._mock_flags(hints_enabled=True):
            show_hint(HintType.REUSE_RELATIONS_ON_TOO_MANY_MODELS)

        mock_fire_event.assert_called_once()
        mock_track_hint_view.assert_called_once()
        # The shown hint is now on cooldown again.
        assert has_hint_cooldown(HintType.REUSE_RELATIONS_ON_TOO_MANY_MODELS) is True
