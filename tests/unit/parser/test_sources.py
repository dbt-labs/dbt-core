from typing import List, Optional

import pytest

from core.dbt.artifacts.resources.v1.components import FreshnessThreshold, Time
from core.dbt.parser.sources import merge_source_freshness


class TestMergeSourceFreshness:
    @pytest.mark.parametrize(
        "thresholds,expected_result",
        [
            ([None, None], None),
            (
                [
                    FreshnessThreshold(
                        warn_after=Time(count=1, period="hour"),
                        error_after=Time(count=1, period="day"),
                    ),
                    None,
                ],
                None,
            ),
            (
                [
                    FreshnessThreshold(
                        warn_after=Time(count=1, period="hour"),
                        error_after=Time(count=1, period="day"),
                    ),
                    None,
                    FreshnessThreshold(),
                ],
                None,
            ),
            (
                [
                    FreshnessThreshold(warn_after=Time(count=1, period="hour")),
                    FreshnessThreshold(error_after=Time(count=1, period="day")),
                ],
                FreshnessThreshold(
                    warn_after=Time(count=1, period="hour"),
                    error_after=Time(count=1, period="day"),
                ),
            ),
            (
                [
                    None,
                    FreshnessThreshold(warn_after=Time(count=1, period="hour")),
                    FreshnessThreshold(error_after=Time(count=1, period="day")),
                ],
                FreshnessThreshold(
                    warn_after=Time(count=1, period="hour"),
                    error_after=Time(count=1, period="day"),
                ),
            ),
            (
                [
                    FreshnessThreshold(
                        warn_after=Time(count=1, period="hour"),
                        error_after=Time(count=1, period="day"),
                    ),
                    FreshnessThreshold(error_after=Time(count=48, period="hour")),
                ],
                FreshnessThreshold(
                    warn_after=Time(count=1, period="hour"),
                    error_after=Time(count=48, period="hour"),
                ),
            ),
        ],
    )
    def test_merge_source_freshness(
        self,
        thresholds: List[Optional[FreshnessThreshold]],
        expected_result: Optional[FreshnessThreshold],
    ):
        result = merge_source_freshness(*thresholds)
        assert result == expected_result
