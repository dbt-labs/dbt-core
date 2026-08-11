import os
import shutil

import pytest
from pytest_mock import MockerFixture

from dbt.hints import (
    HINT_PREFIX,
    LONG_PARSING_WITHOUT_V2_PARSER,
    REUSE_RELATIONS_ON_TOO_MANY_MODELS,
)
from dbt.tests.util import run_dbt
from dbt_common.events.event_catcher import EventCatcher
from dbt_common.events.types import Note

model_sql = "select 1 as id"


@pytest.fixture(autouse=True)
def no_snowplow(mocker: MockerFixture):
    mocker.patch("dbt.hints.track_hint_view")


@pytest.fixture(autouse=True)
def isolated_home(monkeypatch, tmp_path):
    # hint_ts.json lives in the dbt home dir (~/.dbt). Point HOME at a temp dir
    # per test so runs don't touch (or leak the cooldown through) the real ~/.dbt.
    home = tmp_path / "home"
    home.mkdir()
    monkeypatch.setenv("HOME", str(home))
    monkeypatch.setenv("USERPROFILE", str(home))


def hint_events(catcher: EventCatcher, hint_msg: str):
    # Note fires for lots of things; keep only the specific hint we care about.
    return [e for e in catcher.caught_events if e.data.msg == HINT_PREFIX + hint_msg]


class TestReuseRelationsHint:
    @pytest.fixture(scope="class")
    def models(self):
        return {"model_a.sql": model_sql, "model_b.sql": model_sql}

    @pytest.fixture
    def catcher(self):
        return EventCatcher(event_to_catch=Note)

    def test_fires_when_over_threshold(self, project, mocker, catcher):
        mocker.patch("dbt.task.build.BuildTask.REUSE_RELATIONS_HINT_MODEL_THRESHOLD", 0)
        run_dbt(["build"], callbacks=[catcher.catch])
        assert len(hint_events(catcher, REUSE_RELATIONS_ON_TOO_MANY_MODELS)) == 1

    def test_silent_when_under_threshold(self, project, mocker, catcher):
        mocker.patch("dbt.task.build.BuildTask.REUSE_RELATIONS_HINT_MODEL_THRESHOLD", 100)
        run_dbt(["build"], callbacks=[catcher.catch])
        assert hint_events(catcher, REUSE_RELATIONS_ON_TOO_MANY_MODELS) == []

    def test_silent_when_hints_disabled(self, project, mocker, catcher):
        mocker.patch("dbt.task.build.BuildTask.REUSE_RELATIONS_HINT_MODEL_THRESHOLD", 0)
        run_dbt(["build", "--no-hints-enabled"], callbacks=[catcher.catch])
        assert hint_events(catcher, REUSE_RELATIONS_ON_TOO_MANY_MODELS) == []


class TestReuseRelationsHintSkippedWithState:
    # Users on state/deferral aren't building from scratch, so the reuse hint
    # should stay silent even when the model count is over the threshold.
    @pytest.fixture(scope="class")
    def models(self):
        return {"model_a.sql": model_sql, "model_b.sql": model_sql}

    def _make_state(self, project) -> str:
        # `parse` produces a manifest without triggering the reuse hint, so no
        # hint_ts.json is written and the cooldown can't mask the state guard.
        run_dbt(["parse"])
        state = os.path.join(project.project_root, "state")
        os.makedirs(state, exist_ok=True)
        shutil.copyfile(
            os.path.join(project.project_root, "target", "manifest.json"),
            os.path.join(state, "manifest.json"),
        )
        return "state"

    @pytest.mark.parametrize(
        "extra_args",
        [
            ["--state", "state"],
            ["--defer", "--state", "state"],
            ["--defer-state", "state"],
        ],
    )
    def test_silent_when_state_or_defer_in_use(self, project, mocker, extra_args):
        mocker.patch("dbt.task.build.BuildTask.REUSE_RELATIONS_HINT_MODEL_THRESHOLD", 0)
        self._make_state(project)

        catcher = EventCatcher(event_to_catch=Note)
        run_dbt(["build"] + extra_args, callbacks=[catcher.catch])
        assert hint_events(catcher, REUSE_RELATIONS_ON_TOO_MANY_MODELS) == []


class TestHintsDisabledViaProjectFlag:
    # `hints_enabled: false` under `flags:` in dbt_project.yml must suppress hints.
    @pytest.fixture(scope="class")
    def models(self):
        return {"model_a.sql": model_sql, "model_b.sql": model_sql}

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"flags": {"hints_enabled": False}}

    def test_project_flag_disables_hint(self, project, mocker):
        mocker.patch("dbt.task.build.BuildTask.REUSE_RELATIONS_HINT_MODEL_THRESHOLD", 0)
        catcher = EventCatcher(event_to_catch=Note)
        run_dbt(["build"], callbacks=[catcher.catch])
        assert hint_events(catcher, REUSE_RELATIONS_ON_TOO_MANY_MODELS) == []


class TestHintCooldownAcrossRuns:
    @pytest.fixture(scope="class")
    def models(self):
        return {"model_a.sql": model_sql, "model_b.sql": model_sql}

    def test_second_run_is_silent_within_cooldown(self, project, mocker):
        mocker.patch("dbt.task.build.BuildTask.REUSE_RELATIONS_HINT_MODEL_THRESHOLD", 0)

        # First run shows the hint and writes hint_ts.json to the dbt home dir.
        first = EventCatcher(event_to_catch=Note)
        run_dbt(["build"], callbacks=[first.catch])
        assert len(hint_events(first, REUSE_RELATIONS_ON_TOO_MANY_MODELS)) == 1

        # A second invocation re-reads that file and stays quiet during cooldown.
        second = EventCatcher(event_to_catch=Note)
        run_dbt(["build"], callbacks=[second.catch])
        assert hint_events(second, REUSE_RELATIONS_ON_TOO_MANY_MODELS) == []


class TestLongParsingHint:
    @pytest.fixture(scope="class")
    def models(self):
        return {"model_a.sql": model_sql}

    @pytest.fixture
    def catcher(self):
        return EventCatcher(event_to_catch=Note)

    def test_fires_when_parse_is_slow_on_legacy_parser(self, project, mocker, catcher):
        mocker.patch("dbt.parser.manifest.LONG_PARSING_THRESHOLD_SECONDS", -1)
        run_dbt(["parse", "--no-partial-parse"], callbacks=[catcher.catch])
        assert len(hint_events(catcher, LONG_PARSING_WITHOUT_V2_PARSER)) == 1

    def test_silent_when_parse_is_fast(self, project, mocker, catcher):
        mocker.patch("dbt.parser.manifest.LONG_PARSING_THRESHOLD_SECONDS", 100 * 60)
        run_dbt(["parse", "--no-partial-parse"], callbacks=[catcher.catch])
        assert hint_events(catcher, LONG_PARSING_WITHOUT_V2_PARSER) == []

    def test_silent_when_hints_disabled(self, project, mocker, catcher):
        mocker.patch("dbt.parser.manifest.LONG_PARSING_THRESHOLD_SECONDS", -1)
        run_dbt(["parse", "--no-partial-parse", "--no-hints-enabled"], callbacks=[catcher.catch])
        assert hint_events(catcher, LONG_PARSING_WITHOUT_V2_PARSER) == []
