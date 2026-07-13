import pytest
from pytest_mock import MockerFixture

from dbt.hints import (
    HINT_PREFIX,
    LONG_PARSING_WITHOUT_V2_PARSER,
    REUSE_RELATIONS_ON_TOO_MANY_MODELS,
    reset_hint_ts,
)
from dbt.tests.util import run_dbt
from dbt_common.events.event_catcher import EventCatcher
from dbt_common.events.types import Note

model_sql = "select 1 as id"


@pytest.fixture(autouse=True)
def no_snowplow(mocker: MockerFixture):
    mocker.patch("dbt.hints.track_hint_view")


@pytest.fixture(autouse=True)
def fresh_hint_ts():
    # The cooldown cache is a module global that survives across invocations in
    # the same process, so drop it before and after each test to avoid leakage.
    reset_hint_ts()
    yield
    reset_hint_ts()


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


class TestHintCooldownAcrossRuns:
    # Its own class so it gets a fresh (class-scoped) target dir, i.e. a clean
    # hint_ts.json that isn't pre-populated by the other tests above.
    @pytest.fixture(scope="class")
    def models(self):
        return {"model_a.sql": model_sql, "model_b.sql": model_sql}

    def test_second_run_is_silent_within_cooldown(self, project, mocker):
        mocker.patch("dbt.task.build.BuildTask.REUSE_RELATIONS_HINT_MODEL_THRESHOLD", 0)

        # First run shows the hint and writes hint_ts.json to the target dir.
        first = EventCatcher(event_to_catch=Note)
        run_dbt(["build"], callbacks=[first.catch])
        assert len(hint_events(first, REUSE_RELATIONS_ON_TOO_MANY_MODELS)) == 1

        # A fresh invocation re-reads that file and stays quiet during cooldown.
        reset_hint_ts()
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
