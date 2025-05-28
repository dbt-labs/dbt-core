from argparse import Namespace
from datetime import datetime
from typing import Any, Optional, Type, Union
from unittest import mock

import pytest
import pytz

from dbt.adapters.base import BaseRelation
from dbt.artifacts.resources import NodeConfig, Quoting, SeedConfig, SnapshotConfig
from dbt.artifacts.resources.types import BatchSize
from dbt.context.providers import (
    BaseResolver,
    EventTimeFilter,
    RuntimeRefResolver,
    RuntimeSourceResolver,
)
from dbt.contracts.graph.nodes import BatchContext, ModelNode, SnapshotNode
from dbt.event_time.sample_window import SampleWindow
from dbt.flags import set_from_args


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
        "use_microbatch_batches,materialized,incremental_strategy,sample,resolver_model_node,target_type,resolver_model_type,expect_filter",
        [
            # Microbatch model without sample
            (
                True,
                "incremental",
                "microbatch",
                None,
                True,
                NodeConfig,
                ModelNode,
                True,
            ),
            # Microbatch model with sample
            (
                True,
                "incremental",
                "microbatch",
                SampleWindow(
                    start=datetime(2024, 1, 1, tzinfo=pytz.UTC),
                    end=datetime(2025, 1, 1, tzinfo=pytz.UTC),
                ),
                True,
                NodeConfig,
                ModelNode,
                True,
            ),
            # Normal model with sample
            (
                False,
                "table",
                None,
                SampleWindow(
                    start=datetime(2024, 1, 1, tzinfo=pytz.UTC),
                    end=datetime(2025, 1, 1, tzinfo=pytz.UTC),
                ),
                True,
                NodeConfig,
                ModelNode,
                True,
            ),
            # Incremental merge model with sample
            (
                True,
                "incremental",
                "merge",
                SampleWindow(
                    start=datetime(2024, 1, 1, tzinfo=pytz.UTC),
                    end=datetime(2025, 1, 1, tzinfo=pytz.UTC),
                ),
                True,
                NodeConfig,
                ModelNode,
                True,
            ),
            # Sample, but not model node
            (
                False,
                "table",
                None,
                SampleWindow(
                    start=datetime(2024, 1, 1, tzinfo=pytz.UTC),
                    end=datetime(2025, 1, 1, tzinfo=pytz.UTC),
                ),
                False,
                NodeConfig,
                ModelNode,
                False,
            ),
            # Microbatch, but not model node
            (
                True,
                "incremental",
                "microbatch",
                None,
                False,
                NodeConfig,
                ModelNode,
                False,
            ),
            # Mircrobatch model, but not using batches
            (
                False,
                "incremental",
                "microbatch",
                None,
                True,
                NodeConfig,
                ModelNode,
                False,
            ),
            # Non microbatch model, but supposed to use batches
            (
                True,
                "table",
                "microbatch",
                None,
                True,
                NodeConfig,
                ModelNode,
                False,
            ),
            # Incremental merge
            (True, "incremental", "merge", None, True, NodeConfig, ModelNode, False),
            # Target seed node, with sample
            (
                False,
                "table",
                None,
                SampleWindow.from_relative_string("2 days"),
                True,
                SeedConfig,
                ModelNode,
                True,
            ),
            # Target seed node, without sample
            (False, "table", None, None, True, SeedConfig, ModelNode, False),
            # Sample model from snapshot node
            (
                False,
                "table",
                None,
                SampleWindow.from_relative_string("2 days"),
                True,
                NodeConfig,
                SnapshotNode,
                True,
            ),
            # Target model from snapshot, without sample
            (False, "table", None, None, True, NodeConfig, SnapshotNode, False),
            # Target snapshot from model, with sample
            (
                False,
                "table",
                None,
                SampleWindow.from_relative_string("2 days"),
                True,
                SnapshotConfig,
                ModelNode,
                True,
            ),
        ],
    )
    def test_resolve_event_time_filter(
        self,
        resolver: ResolverSubclass,
        use_microbatch_batches: bool,
        materialized: str,
        incremental_strategy: Optional[str],
        sample: Optional[SampleWindow],
        resolver_model_node: bool,
        target_type: Any,
        resolver_model_type: Union[Type[ModelNode], Type[SnapshotNode]],
        expect_filter: bool,
    ) -> None:
        # Target mocking
        target = mock.Mock()
        target.config = mock.MagicMock(target_type)
        target.config.event_time = "created_at"

        # Resolver mocking
        resolver.config.args.EVENT_TIME_END = None
        resolver.config.args.EVENT_TIME_START = None
        resolver.config.args.sample = sample
        if resolver_model_node:
            resolver.model = mock.MagicMock(spec=resolver_model_type)
        resolver.model.batch = BatchContext(
            id="1",
            event_time_start=datetime(2024, 1, 1, tzinfo=pytz.UTC),
            event_time_end=datetime(2025, 1, 1, tzinfo=pytz.UTC),
        )
        resolver.model.config = mock.MagicMock(NodeConfig)
        resolver.model.config.materialized = materialized
        resolver.model.config.incremental_strategy = incremental_strategy
        resolver.model.config.batch_size = BatchSize.day
        resolver.model.config.lookback = 1
        resolver.manifest.use_microbatch_batches = mock.Mock()
        resolver.manifest.use_microbatch_batches.return_value = use_microbatch_batches

        # Try to get an EventTimeFilter
        event_time_filter = resolver.resolve_event_time_filter(target=target)

        if expect_filter:
            assert isinstance(event_time_filter, EventTimeFilter)
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

        set_from_args(
            Namespace(require_batched_execution_for_custom_microbatch_strategy=False), None
        )

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

        set_from_args(
            Namespace(require_batched_execution_for_custom_microbatch_strategy=False), None
        )

        # create limited relation
        relation = resolver.resolve("test", "test")
        assert relation.limit == expected_limit
