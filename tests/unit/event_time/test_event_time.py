from datetime import datetime

import pytest
import pytz

from dbt.artifacts.resources.types import BatchSize
from dbt.event_time.event_time import offset_timestamp


class TestEventTime:

    @pytest.mark.parametrize(
        "timestamp,batch_size,offset,expected_timestamp",
        [
            (
                datetime(2024, 9, 5, 3, 56, 1, 1, pytz.UTC),
                BatchSize.year,
                1,
                datetime(2025, 9, 5, 3, 56, 1, 1, pytz.UTC),
            ),
            (
                datetime(2024, 9, 5, 3, 56, 1, 1, pytz.UTC),
                BatchSize.year,
                -1,
                datetime(2023, 9, 5, 3, 56, 1, 1, pytz.UTC),
            ),
            (
                datetime(2024, 9, 5, 3, 56, 1, 1, pytz.UTC),
                BatchSize.month,
                1,
                datetime(2024, 10, 5, 3, 56, 1, 1, pytz.UTC),
            ),
            (
                datetime(2024, 9, 5, 3, 56, 1, 1, pytz.UTC),
                BatchSize.month,
                -1,
                datetime(2024, 8, 5, 3, 56, 1, 1, pytz.UTC),
            ),
            (
                datetime(2024, 9, 5, 3, 56, 1, 1, pytz.UTC),
                BatchSize.day,
                1,
                datetime(2024, 9, 6, 3, 56, 1, 1, pytz.UTC),
            ),
            (
                datetime(2024, 9, 5, 3, 56, 1, 1, pytz.UTC),
                BatchSize.day,
                -1,
                datetime(2024, 9, 4, 3, 56, 1, 1, pytz.UTC),
            ),
            (
                datetime(2024, 9, 5, 3, 56, 1, 1, pytz.UTC),
                BatchSize.hour,
                1,
                datetime(2024, 9, 5, 4, 56, 1, 1, pytz.UTC),
            ),
            (
                datetime(2024, 9, 5, 3, 56, 1, 1, pytz.UTC),
                BatchSize.hour,
                -1,
                datetime(2024, 9, 5, 2, 56, 1, 1, pytz.UTC),
            ),
            (
                datetime(2024, 1, 31, 16, 6, 0, 0, pytz.UTC),
                BatchSize.month,
                1,
                datetime(2024, 2, 29, 16, 6, 0, 0, pytz.UTC),
            ),
            (
                datetime(2024, 2, 29, 16, 6, 0, 0, pytz.UTC),
                BatchSize.year,
                1,
                datetime(2025, 2, 28, 16, 6, 0, 0, pytz.UTC),
            ),
        ],
    )
    def test_offset_timestamp(self, timestamp, batch_size, offset, expected_timestamp):
        assert offset_timestamp(timestamp, batch_size, offset) == expected_timestamp
