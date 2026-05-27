import datetime
from unittest import mock

import pytest

from dbt.artifacts.schemas.freshness import (
    FreshnessExecutionResultArtifact,
    FreshnessMetadata,
    FreshnessResult,
    PartialSourceFreshnessResult,
    SourceFreshnessRuntimeError,
)
from dbt.artifacts.schemas.results import FreshnessStatus
from dbt.task.freshness import FreshnessResponse, FreshnessRunner, FreshnessTask


class TestFreshnessTaskMetadataCache:
    @pytest.fixture(scope="class")
    def args(self):
        mock_args = mock.Mock()
        mock_args.state = None
        mock_args.defer_state = None
        mock_args.write_json = None

        return mock_args

    @pytest.fixture(scope="class")
    def config(self):
        mock_config = mock.Mock()
        mock_config.threads = 1
        mock_config.target_name = "mock_config_target_name"

    @pytest.fixture(scope="class")
    def manifest(self):
        return mock.Mock()

    @pytest.fixture(scope="class")
    def source_with_loaded_at_field(self):
        mock_source = mock.Mock()
        mock_source.unique_id = "source_with_loaded_at_field"
        mock_source.loaded_at_field = "loaded_at_field"
        return mock_source

    @pytest.fixture(scope="class")
    def source_no_loaded_at_field(self):
        mock_source = mock.Mock()
        mock_source.unique_id = "source_no_loaded_at_field"
        return mock_source

    @pytest.fixture(scope="class")
    def source_no_loaded_at_field2(self):
        mock_source = mock.Mock()
        mock_source.unique_id = "source_no_loaded_at_field2"
        return mock_source

    @pytest.fixture(scope="class")
    def adapter(self):
        return mock.Mock()

    @pytest.fixture(scope="class")
    def freshness_response(self):
        return FreshnessResponse(
            max_loaded_at=datetime.datetime(2020, 5, 2),
            snapshotted_at=datetime.datetime(2020, 5, 4),
            age=2,
        )

    def test_populate_metadata_freshness_cache(
        self, args, config, manifest, adapter, source_no_loaded_at_field, freshness_response
    ):
        manifest.sources = {source_no_loaded_at_field.unique_id: source_no_loaded_at_field}
        adapter.Relation.create_from.return_value = "source_relation"
        adapter.calculate_freshness_from_metadata_batch.return_value = (
            [],
            {"source_relation": freshness_response},
        )
        task = FreshnessTask(args=args, config=config, manifest=manifest, catalogs=[])

        task.populate_metadata_freshness_cache(adapter, {source_no_loaded_at_field.unique_id})

        assert task.get_freshness_metadata_cache() == {"source_relation": freshness_response}

    def test_populate_metadata_freshness_cache_multiple_sources(
        self,
        args,
        config,
        manifest,
        adapter,
        source_no_loaded_at_field,
        source_no_loaded_at_field2,
        freshness_response,
    ):
        manifest.sources = {
            source_no_loaded_at_field.unique_id: source_no_loaded_at_field,
            source_no_loaded_at_field2.unique_id: source_no_loaded_at_field2,
        }
        adapter.Relation.create_from.side_effect = ["source_relation1", "source_relation2"]
        adapter.calculate_freshness_from_metadata_batch.return_value = (
            [],
            {"source_relation1": freshness_response, "source_relation2": freshness_response},
        )
        task = FreshnessTask(args=args, config=config, manifest=manifest, catalogs=[])

        task.populate_metadata_freshness_cache(adapter, {source_no_loaded_at_field.unique_id})

        assert task.get_freshness_metadata_cache() == {
            "source_relation1": freshness_response,
            "source_relation2": freshness_response,
        }

    def test_populate_metadata_freshness_cache_with_loaded_at_field(
        self, args, config, manifest, adapter, source_with_loaded_at_field, freshness_response
    ):
        manifest.sources = {
            source_with_loaded_at_field.unique_id: source_with_loaded_at_field,
        }
        adapter.Relation.create_from.return_value = "source_relation"
        adapter.calculate_freshness_from_metadata_batch.return_value = (
            [],
            {"source_relation": freshness_response},
        )
        task = FreshnessTask(args=args, config=config, manifest=manifest, catalogs=[])

        task.populate_metadata_freshness_cache(adapter, {source_with_loaded_at_field.unique_id})

        assert task.get_freshness_metadata_cache() == {"source_relation": freshness_response}

    def test_populate_metadata_freshness_cache_multiple_sources_mixed(
        self,
        args,
        config,
        manifest,
        adapter,
        source_no_loaded_at_field,
        source_with_loaded_at_field,
        freshness_response,
    ):
        manifest.sources = {
            source_no_loaded_at_field.unique_id: source_no_loaded_at_field,
            source_with_loaded_at_field.unique_id: source_with_loaded_at_field,
        }
        adapter.Relation.create_from.return_value = "source_relation"
        adapter.calculate_freshness_from_metadata_batch.return_value = (
            [],
            {"source_relation": freshness_response},
        )
        task = FreshnessTask(args=args, config=config, manifest=manifest, catalogs=[])

        task.populate_metadata_freshness_cache(adapter, {source_no_loaded_at_field.unique_id})

        assert task.get_freshness_metadata_cache() == {"source_relation": freshness_response}

    def test_populate_metadata_freshness_cache_adapter_exception(
        self, args, config, manifest, adapter, source_no_loaded_at_field, freshness_response
    ):
        manifest.sources = {source_no_loaded_at_field.unique_id: source_no_loaded_at_field}
        adapter.Relation.create_from.return_value = "source_relation"
        adapter.calculate_freshness_from_metadata_batch.side_effect = Exception()
        task = FreshnessTask(args=args, config=config, manifest=manifest, catalogs=[])

        task.populate_metadata_freshness_cache(adapter, {source_no_loaded_at_field.unique_id})

        assert task.get_freshness_metadata_cache() == {}


class TestFreshnessExecutionResultArtifact:
    """Tests for FreshnessExecutionResultArtifact.from_result.

    Regression tests for https://github.com/dbt-labs/dbt-core/issues/12812:
    sources with warn freshness status were missing from sources.json when
    warn_error raised EventCompilationError during after_execute, causing
    _handle_thread_exception to create a PartialSourceFreshnessResult that
    was silently filtered out by the isinstance(r, SourceFreshnessResult) check.
    """

    def _make_partial_result(self, status=FreshnessStatus.RuntimeErr):
        node = mock.Mock()
        node.unique_id = "source.project.my_source.my_table"
        return PartialSourceFreshnessResult(
            status=status,
            thread_id="Thread-1",
            execution_time=0.1,
            timing=[],
            message="Exception on worker thread.",
            node=node,
            adapter_response={},
            failures=None,
        )

    def _make_freshness_result(self, results):
        return FreshnessResult(
            metadata=FreshnessMetadata(),
            results=results,
            elapsed_time=1.0,
        )

    def test_partial_result_included_in_sources_json(self):
        """PartialSourceFreshnessResult (RuntimeErr) must appear in sources.json."""
        partial = self._make_partial_result(FreshnessStatus.RuntimeErr)
        freshness_result = self._make_freshness_result([partial])

        artifact = FreshnessExecutionResultArtifact.from_result(freshness_result)

        assert len(artifact.results) == 1
        assert isinstance(artifact.results[0], SourceFreshnessRuntimeError)
        assert artifact.results[0].unique_id == partial.node.unique_id

    def test_non_freshness_result_excluded_from_sources_json(self):
        """Generic non-freshness results must still be excluded from sources.json."""
        generic = mock.Mock()
        generic.__class__ = object  # not a SourceFreshnessResult or PartialSourceFreshnessResult
        freshness_result = self._make_freshness_result([generic])

        artifact = FreshnessExecutionResultArtifact.from_result(freshness_result)

        assert len(artifact.results) == 0


class TestFreshnessRunnerErrorResult:
    """Tests that FreshnessRunner.error_result returns PartialSourceFreshnessResult.

    This is called by _handle_thread_exception so that thread-level failures
    produce a properly-typed result that is included in sources.json.
    """

    def test_error_result_returns_partial_source_freshness_result(self):
        node = mock.Mock()
        node.unique_id = "source.project.my_source.my_table"

        runner = FreshnessRunner(
            config=mock.Mock(),
            adapter=mock.Mock(),
            node=node,
            node_index=1,
            num_nodes=1,
        )

        result = runner.error_result(node, "something went wrong", datetime.datetime.now().timestamp(), [])

        assert isinstance(result, PartialSourceFreshnessResult)
        assert result.status == FreshnessStatus.RuntimeErr
        assert result.message == "something went wrong"
