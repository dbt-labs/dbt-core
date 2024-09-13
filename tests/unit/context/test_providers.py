import os
from datetime import datetime
from typing import Optional
from unittest import mock

import pytest
import pytz
from freezegun import freeze_time
from pytest_mock import MockerFixture

from dbt.adapters.base import BaseRelation
from dbt.artifacts.resources import NodeConfig, Quoting
from dbt.artifacts.resources.types import BatchSize
from dbt.context.providers import (
    BaseResolver,
    EventTimeFilter,
    RuntimeRefResolver,
    RuntimeSourceResolver,
)


class TestBaseResolver:
    class ResolverSubclass(BaseResolver):
        def __call__(self, *args: str):
            pass

    @pytest.fixture
    def resolver(self):
        return self.ResolverSubclass(
            db_wrapper=mock.Mock(),
            model=mock.Mock(),
            config=mock.Mock(),
            manifest=mock.Mock(),
        )

    @pytest.mark.parametrize(
        "empty,expected_resolve_limit",
        [(False, None), (True, 0)],
    )
    def test_resolve_limit(self, resolver, empty, expected_resolve_limit):
        resolver.config.args.EMPTY = empty

        assert resolver.resolve_limit == expected_resolve_limit

    @pytest.mark.parametrize(
        "dbt_experimental_microbatch,materialized,incremental_strategy,expect_filter",
        [
            (True, "incremental", "microbatch", True),
            (False, "incremental", "microbatch", False),
            (True, "table", "microbatch", False),
            (True, "incremental", "merge", False),
        ],
    )
    def test_resolve_event_time_filter_gating(
        self,
        mocker: MockerFixture,
        resolver: ResolverSubclass,
        dbt_experimental_microbatch: bool,
        materialized: str,
        incremental_strategy: str,
        expect_filter: bool,
    ) -> None:
        if dbt_experimental_microbatch:
            mocker.patch.dict(os.environ, {"DBT_EXPERIMENTAL_MICROBATCH": "True"})

        mocker.patch("dbt.context.providers.BaseResolver._is_incremental").return_value = True

        # Target mocking
        target = mock.Mock()
        target.config = mock.MagicMock(NodeConfig)
        target.config.event_time = "created_at"

        # Resolver mocking
        resolver.config.args.EVENT_TIME_END = None
        resolver.config.args.EVENT_TIME_START = None
        resolver.model.config = mock.MagicMock(NodeConfig)
        resolver.model.config.materialized = materialized
        resolver.model.config.incremental_strategy = incremental_strategy
        resolver.model.config.batch_size = BatchSize.day
        resolver.model.config.lookback = 0

        # Try to get an EventTimeFilter
        event_time_filter = resolver.resolve_event_time_filter(target=target)

        if expect_filter:
            assert isinstance(event_time_filter, EventTimeFilter)
        else:
            assert event_time_filter is None

    @freeze_time("2024-09-05 08:56:00")
    @mock.patch.dict(os.environ, {"DBT_EXPERIMENTAL_MICROBATCH": "True"})
    @pytest.mark.parametrize(
        "event_time_end,event_time_start,expect_filter",
        [
            (None, None, True),
            (datetime(2024, 9, 5), None, True),
            (None, datetime(2024, 9, 4), True),
            (datetime(2024, 9, 5), datetime(2024, 9, 4), True),
        ],
    )
    def test_event_time_filtering_is_incremental_false(
        self,
        mocker: MockerFixture,
        resolver: ResolverSubclass,
        event_time_end: datetime,
        event_time_start: datetime,
        expect_filter: bool,
    ) -> None:
        mocker.patch("dbt.context.providers.BaseResolver._is_incremental").return_value = False

        # Target mocking
        target = mock.Mock()
        target.config = mock.MagicMock(NodeConfig)
        target.config.event_time = "created_at"

        # Resolver mocking
        resolver.config.args.EVENT_TIME_END = event_time_end
        resolver.config.args.EVENT_TIME_START = event_time_start
        resolver.model.config = mock.MagicMock(NodeConfig)
        resolver.model.config.materialized = "incremental"
        resolver.model.config.incremental_strategy = "microbatch"
        resolver.model.config.batch_size = BatchSize.day
        resolver.model.config.lookback = 0

        # Try to get an EventTimeFilter
        event_time_filter = resolver.resolve_event_time_filter(target=target)

        if expect_filter:
            assert isinstance(event_time_filter, EventTimeFilter)
        else:
            assert event_time_filter is None

    @freeze_time("2024-09-05 08:56:00")
    @mock.patch.dict(os.environ, {"DBT_EXPERIMENTAL_MICROBATCH": "True"})
    @pytest.mark.parametrize(
        "event_time_end,event_time_start,batch_size,lookback,expected_end,expected_start",
        [
            (
                None,
                None,
                BatchSize.day,
                0,
                datetime(2024, 9, 5, 8, 56, 0, 0, pytz.UTC),
                datetime(2024, 9, 5, 0, 0, 0, 0, pytz.UTC),
            ),
            (
                datetime(2024, 8, 1, 8, 11),
                None,
                BatchSize.day,
                0,
                datetime(2024, 8, 1, 8, 11, 0, 0, pytz.UTC),
                datetime(2024, 8, 1, 0, 0, 0, 0, pytz.UTC),
            ),
            (
                None,
                datetime(2024, 8, 1),
                BatchSize.day,
                0,
                datetime(2024, 9, 5, 8, 56, 0, 0, pytz.UTC),
                datetime(2024, 8, 1, 0, 0, 0, 0, pytz.UTC),
            ),
            (
                datetime(2024, 9, 1),
                datetime(2024, 8, 1),
                BatchSize.day,
                0,
                datetime(2024, 9, 1, 0, 0, 0, 0, pytz.UTC),
                datetime(2024, 8, 1, 0, 0, 0, 0, pytz.UTC),
            ),
            (
                datetime(2024, 9, 1, 0, 49),
                None,
                BatchSize.hour,
                1,
                datetime(2024, 9, 1, 0, 49, 0, 0, pytz.UTC),
                datetime(2024, 8, 31, 23, 0, 0, 0, pytz.UTC),
            ),
            (
                datetime(2024, 9, 1, 13, 31),
                None,
                BatchSize.day,
                1,
                datetime(2024, 9, 1, 13, 31, 0, 0, pytz.UTC),
                datetime(2024, 8, 31, 0, 0, 0, 0, pytz.UTC),
            ),
            (
                datetime(2024, 1, 23, 12, 30),
                None,
                BatchSize.month,
                1,
                datetime(2024, 1, 23, 12, 30, 0, 0, pytz.UTC),
                datetime(2023, 12, 1, 0, 0, 0, 0, pytz.UTC),
            ),
            (
                datetime(2024, 1, 23, 12, 30),
                None,
                BatchSize.year,
                1,
                datetime(2024, 1, 23, 12, 30, 0, 0, pytz.UTC),
                datetime(2023, 1, 1, 0, 0, 0, 0, pytz.UTC),
            ),
        ],
    )
    def test_resolve_event_time_filter_batch_calculation(
        self,
        mocker: MockerFixture,
        resolver: ResolverSubclass,
        event_time_end: Optional[datetime],
        event_time_start: Optional[datetime],
        batch_size: BatchSize,
        lookback: int,
        expected_end: datetime,
        expected_start: datetime,
    ) -> None:
        event_time = "created_at"

        mocker.patch("dbt.context.providers.BaseResolver._is_incremental").return_value = True

        # Target mocking
        target = mock.Mock()
        target.config = mock.MagicMock(NodeConfig)
        target.config.event_time = event_time

        # Resolver mocking
        resolver.model.config = mock.MagicMock(NodeConfig)
        resolver.model.config.materialized = "incremental"
        resolver.model.config.incremental_strategy = "microbatch"
        resolver.model.config.batch_size = batch_size
        resolver.model.config.lookback = lookback
        resolver.config.args.EVENT_TIME_END = event_time_end
        resolver.config.args.EVENT_TIME_START = event_time_start

        # Get EventTimeFilter
        event_time_filter = resolver.resolve_event_time_filter(target=target)

        assert event_time_filter is not None
        assert event_time_filter.field_name == event_time
        assert event_time_filter.end == expected_end
        assert event_time_filter.start == expected_start


class TestRuntimeRefResolver:
    @pytest.fixture
    def resolver(self):
        mock_db_wrapper = mock.Mock()
        mock_db_wrapper.Relation = BaseRelation

        return RuntimeRefResolver(
            db_wrapper=mock_db_wrapper,
            model=mock.Mock(),
            config=mock.Mock(),
            manifest=mock.Mock(),
        )

    @pytest.mark.parametrize(
        "empty,is_ephemeral_model,expected_limit",
        [
            (False, False, None),
            (True, False, 0),
            (False, True, None),
            (True, True, 0),
        ],
    )
    def test_create_relation_with_empty(self, resolver, empty, is_ephemeral_model, expected_limit):
        # setup resolver and input node
        resolver.config.args.EMPTY = empty
        resolver.config.quoting = {}
        mock_node = mock.Mock()
        mock_node.database = "test"
        mock_node.schema = "test"
        mock_node.identifier = "test"
        mock_node.quoting_dict = {}
        mock_node.alias = "test"
        mock_node.is_ephemeral_model = is_ephemeral_model
        mock_node.defer_relation = None

        # create limited relation
        with mock.patch("dbt.contracts.graph.nodes.ParsedNode", new=mock.Mock):
            relation = resolver.create_relation(mock_node)
        assert relation.limit == expected_limit


class TestRuntimeSourceResolver:
    @pytest.fixture
    def resolver(self):
        mock_db_wrapper = mock.Mock()
        mock_db_wrapper.Relation = BaseRelation

        return RuntimeSourceResolver(
            db_wrapper=mock_db_wrapper,
            model=mock.Mock(),
            config=mock.Mock(),
            manifest=mock.Mock(),
        )

    @pytest.mark.parametrize(
        "empty,expected_limit",
        [
            (False, None),
            (True, 0),
        ],
    )
    def test_create_relation_with_empty(self, resolver, empty, expected_limit):
        # setup resolver and input source
        resolver.config.args.EMPTY = empty
        resolver.config.quoting = {}

        mock_source = mock.Mock()
        mock_source.database = "test"
        mock_source.schema = "test"
        mock_source.identifier = "test"
        mock_source.quoting = Quoting()
        mock_source.quoting_dict = {}
        resolver.manifest.resolve_source.return_value = mock_source

        # create limited relation
        relation = resolver.resolve("test", "test")
        assert relation.limit == expected_limit
