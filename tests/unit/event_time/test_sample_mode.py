from datetime import datetime
from typing import Union

import freezegun
import pytest
import pytz

from dbt.event_time.sample_window import SampleWindow
from dbt_common.exceptions import DbtRuntimeError


@pytest.mark.parametrize(
    "relative_string,expected_result",
    [
        (
            "4 years",
            SampleWindow(
                start=datetime(2021, 1, 28, 18, 4, 0, 0, pytz.UTC),
                end=datetime(2025, 1, 28, 18, 4, 0, 0, pytz.UTC),
            ),
        ),
        (
            "1 year",
            SampleWindow(
                start=datetime(2024, 1, 28, 18, 4, 0, 0, pytz.UTC),
                end=datetime(2025, 1, 28, 18, 4, 0, 0, pytz.UTC),
            ),
        ),
        (
            "4 YEARS",
            SampleWindow(
                start=datetime(2021, 1, 28, 18, 4, 0, 0, pytz.UTC),
                end=datetime(2025, 1, 28, 18, 4, 0, 0, pytz.UTC),
            ),
        ),
        (
            "1 YEAR",
            SampleWindow(
                start=datetime(2024, 1, 28, 18, 4, 0, 0, pytz.UTC),
                end=datetime(2025, 1, 28, 18, 4, 0, 0, pytz.UTC),
            ),
        ),
        (
            "4 months",
            SampleWindow(
                start=datetime(2024, 9, 28, 18, 4, 0, 0, pytz.UTC),
                end=datetime(2025, 1, 28, 18, 4, 0, 0, pytz.UTC),
            ),
        ),
        (
            "1 month",
            SampleWindow(
                start=datetime(2024, 12, 28, 18, 4, 0, 0, pytz.UTC),
                end=datetime(2025, 1, 28, 18, 4, 0, 0, pytz.UTC),
            ),
        ),
        (
            "4 MONTHS",
            SampleWindow(
                start=datetime(2024, 9, 28, 18, 4, 0, 0, pytz.UTC),
                end=datetime(2025, 1, 28, 18, 4, 0, 0, pytz.UTC),
            ),
        ),
        (
            "1 MONTH",
            SampleWindow(
                start=datetime(2024, 12, 28, 18, 4, 0, 0, pytz.UTC),
                end=datetime(2025, 1, 28, 18, 4, 0, 0, pytz.UTC),
            ),
        ),
        (
            "4 days",
            SampleWindow(
                start=datetime(2025, 1, 24, 18, 4, 0, 0, pytz.UTC),
                end=datetime(2025, 1, 28, 18, 4, 0, 0, pytz.UTC),
            ),
        ),
        (
            "1 day",
            SampleWindow(
                start=datetime(2025, 1, 27, 18, 4, 0, 0, pytz.UTC),
                end=datetime(2025, 1, 28, 18, 4, 0, 0, pytz.UTC),
            ),
        ),
        (
            "4 DAYS",
            SampleWindow(
                start=datetime(2025, 1, 24, 18, 4, 0, 0, pytz.UTC),
                end=datetime(2025, 1, 28, 18, 4, 0, 0, pytz.UTC),
            ),
        ),
        (
            "1 DAY",
            SampleWindow(
                start=datetime(2025, 1, 27, 18, 4, 0, 0, pytz.UTC),
                end=datetime(2025, 1, 28, 18, 4, 0, 0, pytz.UTC),
            ),
        ),
        (
            "4 hours",
            SampleWindow(
                start=datetime(2025, 1, 28, 14, 4, 0, 0, pytz.UTC),
                end=datetime(2025, 1, 28, 18, 4, 0, 0, pytz.UTC),
            ),
        ),
        (
            "1 hour",
            SampleWindow(
                start=datetime(2025, 1, 28, 17, 4, 0, 0, pytz.UTC),
                end=datetime(2025, 1, 28, 18, 4, 0, 0, pytz.UTC),
            ),
        ),
        (
            "4 HOURS",
            SampleWindow(
                start=datetime(2025, 1, 28, 14, 4, 0, 0, pytz.UTC),
                end=datetime(2025, 1, 28, 18, 4, 0, 0, pytz.UTC),
            ),
        ),
        (
            "1 HOUR",
            SampleWindow(
                start=datetime(2025, 1, 28, 17, 4, 0, 0, pytz.UTC),
                end=datetime(2025, 1, 28, 18, 4, 0, 0, pytz.UTC),
            ),
        ),
        (
            "1 week",
            DbtRuntimeError(
                "Invalid grain size 'week'. Must be one of ['hour', 'day', 'month', 'year', 'hours', 'days', 'months', 'years']."
            ),
        ),
        ("an hour", DbtRuntimeError("Unable to convert 'an' to an integer.")),
        (
            "3",
            DbtRuntimeError(
                "Cannot load SAMPLE_WINDOW from '3'. Must be of form 'DAYS_INT GRAIN_SIZE'."
            ),
        ),
        (
            "True",
            DbtRuntimeError(
                "Cannot load SAMPLE_WINDOW from 'True'. Must be of form 'DAYS_INT GRAIN_SIZE'."
            ),
        ),
        ("days 3", DbtRuntimeError("Unable to convert 'days' to an integer.")),
        (
            "{}",
            DbtRuntimeError(
                "Cannot load SAMPLE_WINDOW from '{}'. Must be of form 'DAYS_INT GRAIN_SIZE'."
            ),
        ),
    ],
)
@freezegun.freeze_time("2025-01-28T18:04:0Z")
def test_from_relative_string(
    relative_string: str, expected_result: Union[SampleWindow, Exception]
):
    try:
        result = SampleWindow.from_relative_string(relative_string)
        assert result == expected_result
    except Exception as e:
        assert str(e) == str(expected_result)
