import pytest

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
