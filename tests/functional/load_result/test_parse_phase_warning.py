"""Regression tests for #11070.

`load_result` must emit a `JinjaLogWarning` pointing at the `execute` guard
when called during the parse phase with a name that was never stored. The
downstream "'None' has no attribute 'table'" compilation error otherwise
hides the real cause (a missing `{% if execute %}` block).

The warning must NOT fire when the call is properly guarded.
"""

import pytest

from dbt.events.types import JinjaLogWarning
from dbt.tests.util import run_dbt
from dbt_common.events.event_catcher import EventCatcher


# Calls `load_result` from a model body without an `{% if execute %}` guard.
# This is the bug class the warning targets.
unguarded_load_result_sql = """
{% set result = load_result('does_not_exist') %}
select 1 as fun
"""


# Same `load_result` call, but properly wrapped in `{% if execute %}`. The
# warning must stay silent here — firing on guarded calls would punish
# correct user code.
guarded_load_result_sql = """
{% if execute %}
  {% set result = load_result('does_not_exist') %}
{% endif %}
select 1 as fun
"""


class TestLoadResultParsePhaseWarningFires:
    @pytest.fixture(scope="class")
    def models(self):
        return {"unguarded.sql": unguarded_load_result_sql}

    def test_warning_fires_during_parse(self, project) -> None:
        event_catcher = EventCatcher(event_to_catch=JinjaLogWarning)
        run_dbt(["parse"], callbacks=[event_catcher.catch])

        relevant = [
            ev
            for ev in event_catcher.caught_events
            if "load_result('does_not_exist')" in ev.data.msg
        ]
        assert len(relevant) >= 1, (
            f"expected a load_result warning during parse, got: "
            f"{[ev.data.msg for ev in event_catcher.caught_events]}"
        )

        msg = relevant[0].data.msg
        # message identifies the offending model so users can find it
        assert "unguarded" in msg
        # message names the phase and the suggested guard
        assert "parse phase" in msg
        assert "{% if execute %}" in msg
        # message links to the canonical docs page
        assert "docs.getdbt.com" in msg


class TestLoadResultParsePhaseWarningStaysSilentWhenGuarded:
    @pytest.fixture(scope="class")
    def models(self):
        return {"guarded.sql": guarded_load_result_sql}

    def test_warning_does_not_fire_when_guarded(self, project) -> None:
        event_catcher = EventCatcher(event_to_catch=JinjaLogWarning)
        run_dbt(["parse"], callbacks=[event_catcher.catch])

        relevant = [
            ev
            for ev in event_catcher.caught_events
            if "load_result('does_not_exist')" in ev.data.msg
        ]
        assert relevant == [], (
            f"expected no load_result warning when guarded, got: "
            f"{[ev.data.msg for ev in relevant]}"
        )
