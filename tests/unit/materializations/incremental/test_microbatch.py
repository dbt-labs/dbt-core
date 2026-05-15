from datetime import datetime
from unittest import mock

import pytest
import pytz
from freezegun import freeze_time

from dbt.artifacts.resources import NodeConfig
from dbt.artifacts.resources.types import BatchSize
from dbt.materializations.incremental.microbatch import MicrobatchBuilder

MODEL_CONFIG_BEGIN = datetime(2024, 1, 1, 0, 0, 0, 0, pytz.UTC)


class TestMicrobatchBuilder:
    @pytest.fixture(scope="class")
    def microbatch_model(self):
        model = mock.Mock()
        model.config = mock.MagicMock(NodeConfig)
        model.config.materialized = "incremental"
        model.config.incremental_strategy = "microbatch"
        model.config.begin = MODEL_CONFIG_BEGIN
        model.config.batch_size = BatchSize.day

        return model

    @freeze_time("2024-09-05 08:56:00")
    @pytest.mark.parametrize(
        "is_incremental,event_time_end,expected_end_time",
        [
            (
                False,
                None,
                datetime(2024, 9, 6, 0, 0, 0, 0, pytz.UTC),
            ),
            (
                True,
                None,
                datetime(2024, 9, 6, 0, 0, 0, 0, pytz.UTC),
            ),
            (
                False,
                datetime(2024, 10, 1, 0, 0, 0, 0, pytz.UTC),
                datetime(2024, 10, 1, 0, 0, 0, 0, pytz.UTC),
            ),
            (
                True,
                datetime(2024, 10, 1, 0, 0, 0, 0, pytz.UTC),
                datetime(2024, 10, 1, 0, 0, 0, 0, pytz.UTC),
            ),
        ],
    )
    def test_build_end_time(
        self, microbatch_model, is_incremental, event_time_end, expected_end_time
    ):
        microbatch_builder = MicrobatchBuilder(
            model=microbatch_model,
            is_incremental=is_incremental,
            event_time_start=None,
            event_time_end=event_time_end,
        )

        assert microbatch_builder.build_end_time() == expected_end_time

    @pytest.mark.parametrize(
        "is_incremental,event_time_start,checkpoint,batch_size,lookback,expected_start_time",
        [
            (
                False,
                None,
                datetime(2024, 9, 5, 8, 56, 0, 0, pytz.UTC),
                BatchSize.day,
                0,
                # is_incremental: False => model.config.begin
                MODEL_CONFIG_BEGIN,
            ),
            # BatchSize.year
            (
                False,
                datetime(2024, 9, 5, 8, 56, 0, 0, pytz.UTC),
                datetime(2024, 9, 5, 8, 56, 0, 0, pytz.UTC),
                BatchSize.year,
                0,
                datetime(2024, 1, 1, 0, 0, 0, 0, pytz.UTC),
            ),
            (
                False,
                datetime(2024, 9, 5, 8, 56, 0, 0, pytz.UTC),
                datetime(2024, 9, 5, 8, 56, 0, 0, pytz.UTC),
                BatchSize.year,
                # Offset not applied when event_time_start provided
                1,
                datetime(2024, 1, 1, 0, 0, 0, 0, pytz.UTC),
            ),
            (
                False,
                None,
                datetime(2024, 9, 5, 8, 56, 0, 0, pytz.UTC),
                BatchSize.year,
                0,
                # is_incremental=False + no start_time -> model.config.begin
                MODEL_CONFIG_BEGIN,
            ),
            (
                True,
                None,
                datetime(2024, 9, 5, 8, 56, 0, 0, pytz.UTC),
                BatchSize.year,
                0,
                datetime(2024, 1, 1, 0, 0, 0, 0, pytz.UTC),
            ),
            (
                True,
                None,
                datetime(2024, 9, 5, 8, 56, 0, 0, pytz.UTC),
                BatchSize.year,
                1,
                datetime(2023, 1, 1, 0, 0, 0, 0, pytz.UTC),
            ),
            # BatchSize.month
            (
                False,
                datetime(2024, 9, 5, 8, 56, 0, 0, pytz.UTC),
                datetime(2024, 9, 5, 8, 56, 0, 0, pytz.UTC),
                BatchSize.month,
                0,
                datetime(2024, 9, 1, 0, 0, 0, 0, pytz.UTC),
            ),
            (
                False,
                datetime(2024, 9, 5, 8, 56, 0, 0, pytz.UTC),
                datetime(2024, 9, 5, 8, 56, 0, 0, pytz.UTC),
                BatchSize.month,
                # Offset not applied when event_time_start provided
                1,
                datetime(2024, 9, 1, 0, 0, 0, 0, pytz.UTC),
            ),
            (
                False,
                None,
                datetime(2024, 9, 5, 8, 56, 0, 0, pytz.UTC),
                BatchSize.month,
                0,
                # is_incremental=False + no start_time -> model.config.begin
                MODEL_CONFIG_BEGIN,
            ),
            (
                True,
                None,
                datetime(2024, 9, 5, 8, 56, 0, 0, pytz.UTC),
                BatchSize.month,
                0,
                datetime(2024, 9, 1, 0, 0, 0, 0, pytz.UTC),
            ),
            (
                True,
                None,
                datetime(2024, 9, 5, 8, 56, 0, 0, pytz.UTC),
                BatchSize.month,
                1,
                datetime(2024, 8, 1, 0, 0, 0, 0, pytz.UTC),
            ),
            # BatchSize.day
            (
                False,
                datetime(2024, 9, 5, 8, 56, 0, 0, pytz.UTC),
                datetime(2024, 9, 5, 8, 56, 0, 0, pytz.UTC),
                BatchSize.day,
                0,
                datetime(2024, 9, 5, 0, 0, 0, 0, pytz.UTC),
            ),
            (
                False,
                datetime(2024, 9, 5, 8, 56, 0, 0, pytz.UTC),
                datetime(2024, 9, 5, 8, 56, 0, 0, pytz.UTC),
                BatchSize.day,
                # Offset not applied when event_time_start provided
                1,
                datetime(2024, 9, 5, 0, 0, 0, 0, pytz.UTC),
            ),
            (
                False,
                None,
                datetime(2024, 9, 5, 8, 56, 0, 0, pytz.UTC),
                BatchSize.day,
                0,
                # is_incremental=False + no start_time -> model.config.begin
                MODEL_CONFIG_BEGIN,
            ),
            (
                True,
                None,
                datetime(2024, 9, 5, 8, 56, 0, 0, pytz.UTC),
                BatchSize.day,
                0,
                datetime(2024, 9, 5, 0, 0, 0, 0, pytz.UTC),
            ),
            (
                True,
                None,
                datetime(2024, 9, 5, 8, 56, 0, 0, pytz.UTC),
                BatchSize.day,
                1,
                datetime(2024, 9, 4, 0, 0, 0, 0, pytz.UTC),
            ),
            # BatchSize.hour
            (
                False,
                datetime(2024, 9, 5, 8, 56, 0, 0, pytz.UTC),
                datetime(2024, 9, 5, 8, 56, 0, 0, pytz.UTC),
                BatchSize.hour,
                0,
                datetime(2024, 9, 5, 8, 0, 0, 0, pytz.UTC),
            ),
            (
                False,
                datetime(2024, 9, 5, 8, 56, 0, 0, pytz.UTC),
                datetime(2024, 9, 5, 8, 56, 0, 0, pytz.UTC),
                BatchSize.hour,
                # Offset not applied when event_time_start provided
                1,
                datetime(2024, 9, 5, 8, 0, 0, 0, pytz.UTC),
            ),
            (
                False,
                None,
                datetime(2024, 9, 5, 8, 56, 0, 0, pytz.UTC),
                BatchSize.hour,
                0,
                # is_incremental=False + no start_time -> model.config.begin
                MODEL_CONFIG_BEGIN,
            ),
            (
                True,
                None,
                datetime(2024, 9, 5, 8, 56, 0, 0, pytz.UTC),
                BatchSize.hour,
                0,
                datetime(2024, 9, 5, 8, 0, 0, 0, pytz.UTC),
            ),
            (
                True,
                None,
                datetime(2024, 9, 5, 8, 56, 0, 0, pytz.UTC),
                BatchSize.hour,
                1,
                datetime(2024, 9, 5, 7, 0, 0, 0, pytz.UTC),
            ),
            (
                True,
                None,
                datetime(2024, 9, 5, 0, 0, 0, 0, pytz.UTC),
                BatchSize.hour,
                0,
                datetime(2024, 9, 4, 23, 0, 0, 0, pytz.UTC),
            ),
            (
                True,
                None,
                datetime(2024, 9, 5, 0, 0, 0, 0, pytz.UTC),
                BatchSize.hour,
                1,
                datetime(2024, 9, 4, 22, 0, 0, 0, pytz.UTC),
            ),
            (
                True,
                None,
                datetime(2024, 9, 5, 0, 0, 0, 0, pytz.UTC),
                BatchSize.day,
                0,
                datetime(2024, 9, 4, 0, 0, 0, 0, pytz.UTC),
            ),
            (
                True,
                None,
                datetime(2024, 9, 5, 0, 0, 0, 0, pytz.UTC),
                BatchSize.day,
                1,
                datetime(2024, 9, 3, 0, 0, 0, 0, pytz.UTC),
            ),
            (
                True,
                None,
                datetime(2024, 9, 1, 0, 0, 0, 0, pytz.UTC),
                BatchSize.month,
                0,
                datetime(2024, 8, 1, 0, 0, 0, 0, pytz.UTC),
            ),
            (
                True,
                None,
                datetime(2024, 9, 1, 0, 0, 0, 0, pytz.UTC),
                BatchSize.month,
                1,
                datetime(2024, 7, 1, 0, 0, 0, 0, pytz.UTC),
            ),
            (
                True,
                None,
                datetime(2024, 1, 1, 0, 0, 0, 0, pytz.UTC),
                BatchSize.year,
                0,
                datetime(2023, 1, 1, 0, 0, 0, 0, pytz.UTC),
            ),
            (
                True,
                None,
                datetime(2024, 1, 1, 0, 0, 0, 0, pytz.UTC),
                BatchSize.year,
                1,
                datetime(2022, 1, 1, 0, 0, 0, 0, pytz.UTC),
            ),
        ],
    )
    def test_build_start_time(
        self,
        microbatch_model,
        is_incremental,
        event_time_start,
        checkpoint,
        batch_size,
        lookback,
        expected_start_time,
    ):
        microbatch_model.config.batch_size = batch_size
        microbatch_model.config.lookback = lookback
        microbatch_builder = MicrobatchBuilder(
            model=microbatch_model,
            is_incremental=is_incremental,
            event_time_start=event_time_start,
            event_time_end=None,
        )

        assert microbatch_builder.build_start_time(checkpoint) == expected_start_time

    @pytest.mark.parametrize(
        "start,end,batch_size,expected_batches",
        [
            # BatchSize.year
            (
                datetime(2024, 1, 1, 0, 0, 0, 0, pytz.UTC),
                datetime(2026, 1, 7, 3, 56, 0, 0, pytz.UTC),
                BatchSize.year,
                [
                    (
                        datetime(2024, 1, 1, 0, 0, 0, 0, pytz.UTC),
                        datetime(2025, 1, 1, 0, 0, 0, 0, pytz.UTC),
                    ),
                    (
                        datetime(2025, 1, 1, 0, 0, 0, 0, pytz.UTC),
                        datetime(2026, 1, 1, 0, 0, 0, 0, pytz.UTC),
                    ),
                    (
                        datetime(2026, 1, 1, 0, 0, 0, 0, pytz.UTC),
                        datetime(2026, 1, 7, 3, 56, 0, 0, pytz.UTC),
                    ),
                ],
            ),
            # BatchSize.month
            (
                datetime(2024, 9, 1, 0, 0, 0, 0, pytz.UTC),
                datetime(2024, 11, 7, 3, 56, 0, 0, pytz.UTC),
                BatchSize.month,
                [
                    (
                        datetime(2024, 9, 1, 0, 0, 0, 0, pytz.UTC),
                        datetime(2024, 10, 1, 0, 0, 0, 0, pytz.UTC),
                    ),
                    (
                        datetime(2024, 10, 1, 0, 0, 0, 0, pytz.UTC),
                        datetime(2024, 11, 1, 0, 0, 0, 0, pytz.UTC),
                    ),
                    (
                        datetime(2024, 11, 1, 0, 0, 0, 0, pytz.UTC),
                        datetime(2024, 11, 7, 3, 56, 0, 0, pytz.UTC),
                    ),
                ],
            ),
            # BatchSize.day
            (
                datetime(2024, 9, 5, 0, 0, 0, 0, pytz.UTC),
                datetime(2024, 9, 7, 3, 56, 0, 0, pytz.UTC),
                BatchSize.day,
                [
                    (
                        datetime(2024, 9, 5, 0, 0, 0, 0, pytz.UTC),
                        datetime(2024, 9, 6, 0, 0, 0, 0, pytz.UTC),
                    ),
                    (
                        datetime(2024, 9, 6, 0, 0, 0, 0, pytz.UTC),
                        datetime(2024, 9, 7, 0, 0, 0, 0, pytz.UTC),
                    ),
                    (
                        datetime(2024, 9, 7, 0, 0, 0, 0, pytz.UTC),
                        datetime(2024, 9, 7, 3, 56, 0, 0, pytz.UTC),
                    ),
                ],
            ),
            # BatchSize.week
            (
                datetime(2024, 9, 2, 0, 0, 0, 0, pytz.UTC),
                datetime(2024, 9, 18, 3, 56, 0, 0, pytz.UTC),
                BatchSize.week,
                [
                    (
                        datetime(2024, 9, 2, 0, 0, 0, 0, pytz.UTC),
                        datetime(2024, 9, 9, 0, 0, 0, 0, pytz.UTC),
                    ),
                    (
                        datetime(2024, 9, 9, 0, 0, 0, 0, pytz.UTC),
                        datetime(2024, 9, 16, 0, 0, 0, 0, pytz.UTC),
                    ),
                    (
                        datetime(2024, 9, 16, 0, 0, 0, 0, pytz.UTC),
                        datetime(2024, 9, 18, 3, 56, 0, 0, pytz.UTC),
                    ),
                ],
            ),
            # BatchSize.hour
            (
                datetime(2024, 9, 5, 1, 0, 0, 0, pytz.UTC),
                datetime(2024, 9, 5, 3, 56, 0, 0, pytz.UTC),
                BatchSize.hour,
                [
                    (
                        datetime(2024, 9, 5, 1, 0, 0, 0, pytz.UTC),
                        datetime(2024, 9, 5, 2, 0, 0, 0, pytz.UTC),
                    ),
                    (
                        datetime(2024, 9, 5, 2, 0, 0, 0, pytz.UTC),
                        datetime(2024, 9, 5, 3, 0, 0, 0, pytz.UTC),
                    ),
                    (
                        datetime(2024, 9, 5, 3, 0, 0, 0, pytz.UTC),
                        datetime(2024, 9, 5, 3, 56, 0, 0, pytz.UTC),
                    ),
                ],
            ),
            # Test when event_time_end matches the truncated batch size
            (
                datetime(2024, 1, 1, 0, 0, 0, 0, pytz.UTC),
                datetime(2026, 1, 1, 0, 0, 0, 0, pytz.UTC),
                BatchSize.year,
                [
                    (
                        datetime(2024, 1, 1, 0, 0, 0, 0, pytz.UTC),
                        datetime(2025, 1, 1, 0, 0, 0, 0, pytz.UTC),
                    ),
                    (
                        datetime(2025, 1, 1, 0, 0, 0, 0, pytz.UTC),
                        datetime(2026, 1, 1, 0, 0, 0, 0, pytz.UTC),
                    ),
                ],
            ),
            (
                datetime(2024, 9, 1, 0, 0, 0, 0, pytz.UTC),
                datetime(2024, 11, 1, 0, 0, 0, 0, pytz.UTC),
                BatchSize.month,
                [
                    (
                        datetime(2024, 9, 1, 0, 0, 0, 0, pytz.UTC),
                        datetime(2024, 10, 1, 0, 0, 0, 0, pytz.UTC),
                    ),
                    (
                        datetime(2024, 10, 1, 0, 0, 0, 0, pytz.UTC),
                        datetime(2024, 11, 1, 0, 0, 0, 0, pytz.UTC),
                    ),
                ],
            ),
            (
                datetime(2024, 9, 5, 0, 0, 0, 0, pytz.UTC),
                datetime(2024, 9, 7, 0, 0, 0, 0, pytz.UTC),
                BatchSize.day,
                [
                    (
                        datetime(2024, 9, 5, 0, 0, 0, 0, pytz.UTC),
                        datetime(2024, 9, 6, 0, 0, 0, 0, pytz.UTC),
                    ),
                    (
                        datetime(2024, 9, 6, 0, 0, 0, 0, pytz.UTC),
                        datetime(2024, 9, 7, 0, 0, 0, 0, pytz.UTC),
                    ),
                ],
            ),
            (
                datetime(2024, 9, 5, 1, 0, 0, 0, pytz.UTC),
                datetime(2024, 9, 5, 3, 0, 0, 0, pytz.UTC),
                BatchSize.hour,
                [
                    (
                        datetime(2024, 9, 5, 1, 0, 0, 0, pytz.UTC),
                        datetime(2024, 9, 5, 2, 0, 0, 0, pytz.UTC),
                    ),
                    (
                        datetime(2024, 9, 5, 2, 0, 0, 0, pytz.UTC),
                        datetime(2024, 9, 5, 3, 0, 0, 0, pytz.UTC),
                    ),
                ],
            ),
        ],
    )
    def test_build_batches(self, microbatch_model, start, end, batch_size, expected_batches):
        microbatch_model.config.batch_size = batch_size
        microbatch_builder = MicrobatchBuilder(
            model=microbatch_model, is_incremental=True, event_time_start=None, event_time_end=None
        )

        actual_batches = microbatch_builder.build_batches(start, end)
        assert len(actual_batches) == len(expected_batches)
        assert actual_batches == expected_batches

    def test_build_jinja_context_for_incremental_batch(self, microbatch_model):
        context = MicrobatchBuilder.build_jinja_context_for_batch(
            model=microbatch_model,
            incremental_batch=True,
        )

        assert context["model"] == microbatch_model.to_dict()
        assert context["sql"] == microbatch_model.compiled_code
        assert context["compiled_code"] == microbatch_model.compiled_code

        assert context["is_incremental"]() is True
        assert context["should_full_refresh"]() is False

    def test_build_jinja_context_for_incremental_batch_false(self, microbatch_model):
        context = MicrobatchBuilder.build_jinja_context_for_batch(
            model=microbatch_model,
            incremental_batch=False,
        )

        assert context["model"] == microbatch_model.to_dict()
        assert context["sql"] == microbatch_model.compiled_code
        assert context["compiled_code"] == microbatch_model.compiled_code

        # Only build is_incremental callables when not first batch
        assert "is_incremental" not in context
        assert "should_full_refresh" not in context

    @pytest.mark.parametrize(
        "timestamp,batch_size,offset,expected_timestamp",
        [
            (
                datetime(2024, 9, 5, 3, 56, 1, 1, pytz.UTC),
                BatchSize.year,
                1,
                datetime(2025, 1, 1, 0, 0, 0, 0, pytz.UTC),
            ),
            (
                datetime(2024, 9, 5, 3, 56, 1, 1, pytz.UTC),
                BatchSize.year,
                -1,
                datetime(2023, 1, 1, 0, 0, 0, 0, pytz.UTC),
            ),
            (
                datetime(2024, 9, 5, 3, 56, 1, 1, pytz.UTC),
                BatchSize.month,
                1,
                datetime(2024, 10, 1, 0, 0, 0, 0, pytz.UTC),
            ),
            (
                datetime(2024, 9, 5, 3, 56, 1, 1, pytz.UTC),
                BatchSize.month,
                -1,
                datetime(2024, 8, 1, 0, 0, 0, 0, pytz.UTC),
            ),
            (
                datetime(2024, 9, 5, 3, 56, 1, 1, pytz.UTC),
                BatchSize.week,
                1,
                datetime(2024, 9, 9, 0, 0, 0, 0, pytz.UTC),
            ),
            (
                datetime(2024, 9, 5, 3, 56, 1, 1, pytz.UTC),
                BatchSize.week,
                -1,
                datetime(2024, 8, 26, 0, 0, 0, 0, pytz.UTC),
            ),
            (
                datetime(2024, 9, 5, 3, 56, 1, 1, pytz.UTC),
                BatchSize.day,
                1,
                datetime(2024, 9, 6, 0, 0, 0, 0, pytz.UTC),
            ),
            (
                datetime(2024, 9, 5, 3, 56, 1, 1, pytz.UTC),
                BatchSize.day,
                -1,
                datetime(2024, 9, 4, 0, 0, 0, 0, pytz.UTC),
            ),
            (
                datetime(2024, 9, 5, 3, 56, 1, 1, pytz.UTC),
                BatchSize.hour,
                1,
                datetime(2024, 9, 5, 4, 0, 0, 0, pytz.UTC),
            ),
            (
                datetime(2024, 9, 5, 3, 56, 1, 1, pytz.UTC),
                BatchSize.hour,
                -1,
                datetime(2024, 9, 5, 2, 0, 0, 0, pytz.UTC),
            ),
        ],
    )
    def test_offset_timestamp(self, timestamp, batch_size, offset, expected_timestamp):
        assert (
            MicrobatchBuilder.offset_timestamp(timestamp, batch_size, offset) == expected_timestamp
        )

    @pytest.mark.parametrize(
        "timestamp,batch_size,expected_timestamp",
        [
            (
                datetime(2024, 9, 5, 3, 56, 1, 1, pytz.UTC),
                BatchSize.year,
                datetime(2024, 1, 1, 0, 0, 0, 0, pytz.UTC),
            ),
            (
                datetime(2024, 9, 5, 3, 56, 1, 1, pytz.UTC),
                BatchSize.month,
                datetime(2024, 9, 1, 0, 0, 0, 0, pytz.UTC),
            ),
            (
                datetime(2024, 9, 5, 3, 56, 1, 1, pytz.UTC),
                BatchSize.week,
                datetime(2024, 9, 2, 0, 0, 0, 0, pytz.UTC),
            ),
            (
                datetime(2024, 9, 5, 3, 56, 1, 1, pytz.UTC),
                BatchSize.day,
                datetime(2024, 9, 5, 0, 0, 0, 0, pytz.UTC),
            ),
            (
                datetime(2024, 9, 5, 3, 56, 1, 1, pytz.UTC),
                BatchSize.hour,
                datetime(2024, 9, 5, 3, 0, 0, 0, pytz.UTC),
            ),
        ],
    )
    def test_truncate_timestamp(self, timestamp, batch_size, expected_timestamp):
        assert MicrobatchBuilder.truncate_timestamp(timestamp, batch_size) == expected_timestamp

    @pytest.mark.parametrize(
        "batch_size,start_time,expected_formatted_start_time",
        [
            (BatchSize.year, datetime(2020, 1, 1, 1), "2020"),
            (BatchSize.month, datetime(2020, 1, 1, 1), "202001"),
            (BatchSize.week, datetime(2020, 1, 6, 1), "2020W02"),
            (BatchSize.day, datetime(2020, 1, 1, 1), "20200101"),
            (BatchSize.hour, datetime(2020, 1, 1, 1), "20200101T01"),
        ],
    )
    def test_batch_id(
        self, batch_size: BatchSize, start_time: datetime, expected_formatted_start_time: str
    ) -> None:
        assert MicrobatchBuilder.batch_id(start_time, batch_size) == expected_formatted_start_time

    @pytest.mark.parametrize(
        "batch_size,batch_start,expected_formatted_batch_start",
        [
            (BatchSize.year, datetime(2020, 1, 1, 1), "2020"),
            (BatchSize.month, datetime(2020, 1, 1, 1), "2020-01"),
            (BatchSize.week, datetime(2020, 1, 6, 1), "2020-W02"),
            (BatchSize.day, datetime(2020, 1, 1, 1), "2020-01-01"),
            (BatchSize.hour, datetime(2020, 1, 1, 1), "2020-01-01T01"),
        ],
    )
    def test_format_batch_start(
        self, batch_size: BatchSize, batch_start: datetime, expected_formatted_batch_start: str
    ) -> None:
        assert (
            MicrobatchBuilder.format_batch_start(batch_start, batch_size)
            == expected_formatted_batch_start
        )

    @pytest.mark.parametrize(
        "timestamp,batch_size,expected_datetime",
        [
            (
                datetime(2024, 9, 17, 16, 6, 0, 0, pytz.UTC),
                BatchSize.hour,
                datetime(2024, 9, 17, 17, 0, 0, 0, pytz.UTC),
            ),
            (
                datetime(2024, 9, 17, 16, 0, 0, 0, pytz.UTC),
                BatchSize.hour,
                datetime(2024, 9, 17, 16, 0, 0, 0, pytz.UTC),
            ),
            (
                datetime(2024, 9, 17, 16, 6, 0, 0, pytz.UTC),
                BatchSize.day,
                datetime(2024, 9, 18, 0, 0, 0, 0, pytz.UTC),
            ),
            (
                datetime(2024, 9, 17, 0, 0, 0, 0, pytz.UTC),
                BatchSize.day,
                datetime(2024, 9, 17, 0, 0, 0, 0, pytz.UTC),
            ),
            (
                datetime(2024, 9, 17, 16, 6, 0, 0, pytz.UTC),
                BatchSize.week,
                datetime(2024, 9, 23, 0, 0, 0, 0, pytz.UTC),
            ),
            (
                datetime(2024, 9, 16, 0, 0, 0, 0, pytz.UTC),
                BatchSize.week,
                datetime(2024, 9, 16, 0, 0, 0, 0, pytz.UTC),
            ),
            (
                datetime(2024, 9, 17, 16, 6, 0, 0, pytz.UTC),
                BatchSize.month,
                datetime(2024, 10, 1, 0, 0, 0, 0, pytz.UTC),
            ),
            (
                datetime(2024, 9, 1, 0, 0, 0, 0, pytz.UTC),
                BatchSize.month,
                datetime(2024, 9, 1, 0, 0, 0, 0, pytz.UTC),
            ),
            (
                datetime(2024, 9, 17, 16, 6, 0, 0, pytz.UTC),
                BatchSize.year,
                datetime(2025, 1, 1, 0, 0, 0, 0, pytz.UTC),
            ),
            (
                datetime(2024, 1, 1, 0, 0, 0, 0, pytz.UTC),
                BatchSize.year,
                datetime(2024, 1, 1, 0, 0, 0, 0, pytz.UTC),
            ),
        ],
    )
    def test_ceiling_timestamp(
        self, timestamp: datetime, batch_size: BatchSize, expected_datetime: datetime
    ) -> None:
        ceilinged = MicrobatchBuilder.ceiling_timestamp(timestamp, batch_size)
        assert ceilinged == expected_datetime

    @pytest.mark.parametrize(
        "timestamp,week_start,expected_timestamp",
        [
            # week_start=0 (Monday) - same as default behavior
            (
                datetime(2024, 9, 5, 3, 56, 1, 1, pytz.UTC),  # Thursday
                0,
                datetime(2024, 9, 2, 0, 0, 0, 0, pytz.UTC),  # Monday
            ),
            # week_start=6 (Sunday)
            (
                datetime(2024, 9, 5, 3, 56, 1, 1, pytz.UTC),  # Thursday
                6,
                datetime(2024, 9, 1, 0, 0, 0, 0, pytz.UTC),  # Sunday
            ),
            # week_start=6 (Sunday) when timestamp is Sunday (no change)
            (
                datetime(2024, 9, 1, 0, 0, 0, 0, pytz.UTC),  # Sunday
                6,
                datetime(2024, 9, 1, 0, 0, 0, 0, pytz.UTC),  # Sunday
            ),
            # week_start=6 (Sunday) when timestamp is Monday
            (
                datetime(2024, 9, 2, 0, 0, 0, 0, pytz.UTC),  # Monday
                6,
                datetime(2024, 9, 1, 0, 0, 0, 0, pytz.UTC),  # Sunday
            ),
            # week_start=6 (Sunday) when timestamp is Saturday
            (
                datetime(2024, 9, 7, 0, 0, 0, 0, pytz.UTC),  # Saturday
                6,
                datetime(2024, 9, 1, 0, 0, 0, 0, pytz.UTC),  # Sunday
            ),
            # week_start=1 (Tuesday)
            (
                datetime(2024, 9, 5, 3, 56, 1, 1, pytz.UTC),  # Thursday
                1,
                datetime(2024, 9, 3, 0, 0, 0, 0, pytz.UTC),  # Tuesday
            ),
        ],
    )
    def test_truncate_timestamp_week_start(
        self, timestamp: datetime, week_start: int, expected_timestamp: datetime
    ) -> None:
        assert (
            MicrobatchBuilder.truncate_timestamp(timestamp, BatchSize.week, week_start)
            == expected_timestamp
        )

    @pytest.mark.parametrize(
        "timestamp,week_start,offset,expected_timestamp",
        [
            # week_start=6 (Sunday), offset +1
            (
                datetime(2024, 9, 5, 3, 56, 1, 1, pytz.UTC),  # Thursday
                6,
                1,
                datetime(2024, 9, 8, 0, 0, 0, 0, pytz.UTC),  # next Sunday
            ),
            # week_start=6 (Sunday), offset -1
            (
                datetime(2024, 9, 5, 3, 56, 1, 1, pytz.UTC),  # Thursday
                6,
                -1,
                datetime(2024, 8, 25, 0, 0, 0, 0, pytz.UTC),  # previous Sunday
            ),
        ],
    )
    def test_offset_timestamp_week_start(
        self,
        timestamp: datetime,
        week_start: int,
        offset: int,
        expected_timestamp: datetime,
    ) -> None:
        assert (
            MicrobatchBuilder.offset_timestamp(timestamp, BatchSize.week, offset, week_start)
            == expected_timestamp
        )

    @pytest.mark.parametrize(
        "timestamp,week_start,expected_datetime",
        [
            # week_start=6 (Sunday), timestamp not on boundary
            (
                datetime(2024, 9, 5, 3, 56, 1, 1, pytz.UTC),  # Thursday
                6,
                datetime(2024, 9, 8, 0, 0, 0, 0, pytz.UTC),  # next Sunday
            ),
            # week_start=6 (Sunday), timestamp already on Sunday boundary
            (
                datetime(2024, 9, 1, 0, 0, 0, 0, pytz.UTC),  # Sunday
                6,
                datetime(2024, 9, 1, 0, 0, 0, 0, pytz.UTC),  # same Sunday
            ),
        ],
    )
    def test_ceiling_timestamp_week_start(
        self, timestamp: datetime, week_start: int, expected_datetime: datetime
    ) -> None:
        assert (
            MicrobatchBuilder.ceiling_timestamp(timestamp, BatchSize.week, week_start)
            == expected_datetime
        )

    @pytest.mark.parametrize(
        "week_start,batch_start,expected_formatted_batch_start",
        [
            # week_start=0 (Monday) uses ISO week format
            (0, datetime(2020, 1, 6, 0), "2020-W02"),
            # week_start=6 (Sunday) uses date format
            (6, datetime(2020, 1, 5, 0), "2020-01-05"),
            # week_start=1 (Tuesday) uses date format
            (1, datetime(2020, 1, 7, 0), "2020-01-07"),
        ],
    )
    def test_format_batch_start_week_start(
        self,
        week_start: int,
        batch_start: datetime,
        expected_formatted_batch_start: str,
    ) -> None:
        assert (
            MicrobatchBuilder.format_batch_start(batch_start, BatchSize.week, week_start)
            == expected_formatted_batch_start
        )

    @pytest.mark.parametrize(
        "week_start,batch_start,expected_batch_id",
        [
            # week_start=0 (Monday) uses ISO week format (no dashes)
            (0, datetime(2020, 1, 6, 0), "2020W02"),
            # week_start=6 (Sunday) uses date format (no dashes)
            (6, datetime(2020, 1, 5, 0), "20200105"),
        ],
    )
    def test_batch_id_week_start(
        self,
        week_start: int,
        batch_start: datetime,
        expected_batch_id: str,
    ) -> None:
        assert (
            MicrobatchBuilder.batch_id(batch_start, BatchSize.week, week_start)
            == expected_batch_id
        )

    def test_build_batches_week_start_sunday(self, microbatch_model):
        """Test that build_batches respects week_start=6 (Sunday) for weekly batches."""
        microbatch_model.config.batch_size = BatchSize.week
        microbatch_model.config.week_start = 6  # Sunday
        microbatch_builder = MicrobatchBuilder(
            model=microbatch_model, is_incremental=True, event_time_start=None, event_time_end=None
        )

        # 2024-09-01 is a Sunday; end date is 2024-09-18 (a Wednesday)
        start = datetime(2024, 9, 1, 0, 0, 0, 0, pytz.UTC)
        end = datetime(2024, 9, 18, 3, 56, 0, 0, pytz.UTC)
        batches = microbatch_builder.build_batches(start, end)

        expected_batches = [
            (
                datetime(2024, 9, 1, 0, 0, 0, 0, pytz.UTC),
                datetime(2024, 9, 8, 0, 0, 0, 0, pytz.UTC),
            ),
            (
                datetime(2024, 9, 8, 0, 0, 0, 0, pytz.UTC),
                datetime(2024, 9, 15, 0, 0, 0, 0, pytz.UTC),
            ),
            (
                datetime(2024, 9, 15, 0, 0, 0, 0, pytz.UTC),
                datetime(2024, 9, 18, 3, 56, 0, 0, pytz.UTC),
            ),
        ]
        assert len(batches) == len(expected_batches)
        assert batches == expected_batches
