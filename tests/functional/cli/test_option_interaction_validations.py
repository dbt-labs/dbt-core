from datetime import datetime

import pytest
from freezegun import freeze_time

from dbt.tests.util import run_dbt


class TestEventTimeEndEventTimeStart:
    @pytest.mark.parametrize(
        "event_time_start,event_time_end,expect_pass",
        [
            ("2024-10-01", "2024-10-02", True),
            ("2024-10-02", "2024-10-01", False),
        ],
    )
    def test_option_combo(self, project, event_time_start, event_time_end, expect_pass):
        try:
            run_dbt(
                [
                    "build",
                    "--event-time-start",
                    event_time_start,
                    "--event-time-end",
                    event_time_end,
                ]
            )
            assert expect_pass
        except Exception as e:
            assert (
                "Value for `--event-time-start` must be less than `--event-time-end`"
                in e.__str__()
            )
            assert not expect_pass


class TestEventTimeStartCurrent_time:
    @pytest.mark.parametrize(
        "event_time_start,current_time,expect_pass",
        [
            ("2024-10-01", "2024-10-02", True),
            ("2024-10-02", "2024-10-01", False),
        ],
    )
    def test_option_combo(self, project, event_time_start, current_time, expect_pass):
        with freeze_time(datetime.fromisoformat(current_time)):
            try:
                run_dbt(["build", "--event-time-start", event_time_start])
                assert expect_pass
            except Exception as e:
                assert "must be less than the current time" in e.__str__()
                assert not expect_pass
