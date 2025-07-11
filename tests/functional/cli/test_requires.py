import os

import pytest
from pytest_mock import MockerFixture

from dbt.events.types import JinjaLogInfo
from dbt.tests.util import run_dbt
from tests.utils import EventCatcher

model_one_sql = """
    {{ log("DBT_ENGINE_SHOW_RESOURCE_REPORT: " ~ env_var('DBT_ENGINE_SHOW_RESOURCE_REPORT', default="0"), info=True) }}
    {{ log("DBT_SHOW_RESOURCE_REPORT: " ~ env_var('DBT_SHOW_RESOURCE_REPORT', default="0"), info=True) }}
    select 1 as fun
"""


class TestOldEngineEnvVarPropagation:
    @pytest.fixture(scope="class")
    def models(self):
        return {"model_one.sql": model_one_sql}

    @pytest.mark.parametrize(
        "set_old,set_new, expect",
        [(False, False, 0), (True, False, False), (False, True, True), (True, True, True)],
    )
    def test_engine_env_var_propagation(
        self, project, mocker: MockerFixture, set_old: bool, set_new: bool, expect: bool
    ):
        # Of note, the default value for DBT_PARTIAL_PARSE is True
        if set_old:
            mocker.patch.dict(os.environ, {"DBT_SHOW_RESOURCE_REPORT": "False"})
        if set_new:
            mocker.patch.dict(os.environ, {"DBT_ENGINE_SHOW_RESOURCE_REPORT": "True"})

        event_catcher = EventCatcher(event_to_catch=JinjaLogInfo)
        run_dbt(["parse", "--no-partial-parse"], callbacks=[event_catcher.catch])

        assert len(event_catcher.caught_events) == 2

        for event in event_catcher.caught_events:
            if event.data.msg.startswith("DBT_ENGINE_SHOW_RESOURCE_REPORT"):
                assert event.data.msg.endswith(f"{expect}")
            elif event.data.msg.startswith("DBT_SHOW_RESOURCE_REPORT"):
                assert event.data.msg.endswith(f"{expect}")
            else:
                assert False, "Unexpected log message"
