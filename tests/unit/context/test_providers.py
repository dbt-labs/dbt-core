from datetime import datetime
from typing import Optional
from unittest import mock

import pytest
import pytz
from freezegun import freeze_time
from pytest_mock import MockerFixture

from dbt.adapters.base import BaseRelation
from dbt.artifacts.resources import NodeConfig, Quoting
from dbt.artifacts.resources.types import PartitionGrain
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

    @freeze_time("2024-09-05 08:56:00")
    @pytest.mark.parametrize(
        "is_incremental,materialized,incremental_strategy,event_time_end,event_time_start,batch_size,lookback,expected_filter",
        [
            (True, "table", "microbatch", None, None, PartitionGrain.day, 0, None),
            (True, "incremental", "merge", None, None, PartitionGrain.day, 0, None),
            (
                True,
                "incremental",
                "microbatch",
                None,
                None,
                PartitionGrain.day,
                0,
                EventTimeFilter(
                    field_name="created_at",
                    end=datetime(2024, 9, 5, 8, 56, 0, 0, pytz.UTC),
                    start=datetime(2024, 9, 5, 0, 0, 0, 0, pytz.UTC),
                ),
            ),
            (
                True,
                "incremental",
                "microbatch",
                "2024-08-01 08:11:00",
                None,
                PartitionGrain.day,
                0,
                EventTimeFilter(
                    field_name="created_at",
                    end=datetime(2024, 8, 1, 8, 11, 0, 0, pytz.UTC),
                    start=datetime(2024, 8, 1, 0, 0, 0, 0, pytz.UTC),
                ),
            ),
            (
                True,
                "incremental",
                "microbatch",
                None,
                "2024-08-01 00:00:00",
                PartitionGrain.day,
                0,
                EventTimeFilter(
                    field_name="created_at",
                    end=datetime(2024, 9, 5, 8, 56, 0, 0, pytz.UTC),
                    start=datetime(2024, 8, 1, 0, 0, 0, 0, pytz.UTC),
                ),
            ),
            (
                True,
                "incremental",
                "microbatch",
                "2024-09-01 00:00:00",
                "2024-08-01 00:00:00",
                PartitionGrain.day,
                0,
                EventTimeFilter(
                    field_name="created_at",
                    end=datetime(2024, 9, 1, 0, 0, 0, 0, pytz.UTC),
                    start=datetime(2024, 8, 1, 0, 0, 0, 0, pytz.UTC),
                ),
            ),
            (False, "incremental", "microbatch", None, None, PartitionGrain.day, 0, None),
            (
                False,
                "incremental",
                "microbatch",
                "2024-08-01 08:11:00",
                None,
                PartitionGrain.day,
                0,
                EventTimeFilter(
                    field_name="created_at",
                    end=datetime(2024, 8, 1, 8, 11, 0, 0, pytz.UTC),
                    start=None,
                ),
            ),
            (
                False,
                "incremental",
                "microbatch",
                None,
                "2024-08-01 00:00:00",
                PartitionGrain.day,
                0,
                EventTimeFilter(
                    field_name="created_at",
                    end=None,
                    start=datetime(2024, 8, 1, 0, 0, 0, 0, pytz.UTC),
                ),
            ),
            (
                False,
                "incremental",
                "microbatch",
                "2024-09-01 00:00:00",
                "2024-08-01 00:00:00",
                PartitionGrain.day,
                0,
                EventTimeFilter(
                    field_name="created_at",
                    end=datetime(2024, 9, 1, 0, 0, 0, 0, pytz.UTC),
                    start=datetime(2024, 8, 1, 0, 0, 0, 0, pytz.UTC),
                ),
            ),
            (
                True,
                "incremental",
                "microbatch",
                "2024-09-01 00:49:00",
                None,
                PartitionGrain.hour,
                1,
                EventTimeFilter(
                    field_name="created_at",
                    end=datetime(2024, 9, 1, 0, 49, 0, 0, pytz.UTC),
                    start=datetime(2024, 8, 31, 23, 0, 0, 0, pytz.UTC),
                ),
            ),
            (
                True,
                "incremental",
                "microbatch",
                "2024-09-01 13:31:00",
                None,
                PartitionGrain.day,
                1,
                EventTimeFilter(
                    field_name="created_at",
                    end=datetime(2024, 9, 1, 13, 31, 0, 0, pytz.UTC),
                    start=datetime(2024, 8, 31, 0, 0, 0, 0, pytz.UTC),
                ),
            ),
            (
                True,
                "incremental",
                "microbatch",
                "2024-01-23 12:30:00",
                None,
                PartitionGrain.month,
                1,
                EventTimeFilter(
                    field_name="created_at",
                    end=datetime(2024, 1, 23, 12, 30, 0, 0, pytz.UTC),
                    start=datetime(2023, 12, 1, 0, 0, 0, 0, pytz.utc),
                ),
            ),
            (
                True,
                "incremental",
                "microbatch",
                "2024-01-23 12:30:00",
                None,
                PartitionGrain.year,
                1,
                EventTimeFilter(
                    field_name="created_at",
                    end=datetime(2024, 1, 23, 12, 30, 0, 0, pytz.UTC),
                    start=datetime(2023, 1, 1, 0, 0, 0, 0, pytz.utc),
                ),
            ),
        ],
    )
    def test_resolve_event_time_filter(
        self,
        mocker: MockerFixture,
        resolver: ResolverSubclass,
        is_incremental: bool,
        materialized: str,
        incremental_strategy: str,
        event_time_end: Optional[str],
        event_time_start: Optional[str],
        batch_size: PartitionGrain,
        lookback: int,
        expected_filter: Optional[EventTimeFilter],
    ) -> None:
        mocker.patch("dbt.context.providers.BaseResolver._is_incremental").return_value = (
            is_incremental
        )
        target = mock.Mock()
        target.config = mock.MagicMock(NodeConfig)
        target.config.event_time = "created_at"
        resolver.model.config = mock.MagicMock(NodeConfig)
        resolver.model.config.materialized = materialized
        resolver.model.config.incremental_strategy = incremental_strategy
        resolver.model.config.batch_size = batch_size
        resolver.model.config.lookback = lookback
        resolver.config.args.EVENT_TIME_END = event_time_end
        resolver.config.args.EVENT_TIME_START = event_time_start
        event_time_filter = resolver.resolve_event_time_filter(target=target)

        if expected_filter is not None:
            assert event_time_filter is not None
            assert event_time_filter.field_name == expected_filter.field_name
            assert event_time_filter.end == expected_filter.end
            assert event_time_filter.start == expected_filter.start
        else:
            assert event_time_filter is None


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
