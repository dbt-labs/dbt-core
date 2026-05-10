import datetime
from unittest import mock

import pytest

from dbt.task.freshness import FreshnessResponse, FreshnessTask


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


class TestFreshnessTaskEndMessages:
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
        return mock_config

    @pytest.fixture(scope="class")
    def manifest(self):
        return mock.Mock()

    def _create_result(self, status, source_name="test_source", table_name="test_table"):
        """Helper to create a mock freshness result"""
        result = mock.Mock()
        result.status = status
        result.node = mock.Mock()
        result.node.source_name = source_name
        result.node.name = table_name
        return result

    def test_task_end_messages_all_pass(self, args, config, manifest):
        """Test that StatsLine and FreshnessCheckComplete fire when all checks pass"""
        from dbt.artifacts.schemas.freshness import FreshnessStatus

        task = FreshnessTask(args=args, config=config, manifest=manifest)
        results = [
            self._create_result(FreshnessStatus.Pass, "source1", "table1"),
            self._create_result(FreshnessStatus.Pass, "source2", "table2"),
        ]
        with mock.patch("dbt.task.freshness.fire_event") as mock_fire:
            task.task_end_messages(results)
            fired_types = [type(call.args[0]).__name__ for call in mock_fire.call_args_list]
            # StatsLine and FreshnessCheckComplete should always fire
            assert "StatsLine" in fired_types
            assert "FreshnessCheckComplete" in fired_types
            # No errors or warnings, so EndOfRunSummary should not fire
            assert "EndOfRunSummary" not in fired_types

    def test_task_end_messages_some_warnings(self, args, config, manifest):
        """Test that EndOfRunSummary fires when there are warnings"""
        from dbt.artifacts.schemas.freshness import FreshnessStatus

        task = FreshnessTask(args=args, config=config, manifest=manifest)
        results = [
            self._create_result(FreshnessStatus.Pass, "source1", "table1"),
            self._create_result(FreshnessStatus.Warn, "source2", "table2"),
        ]
        with mock.patch("dbt.task.freshness.fire_event") as mock_fire:
            task.task_end_messages(results)
            fired_types = [type(call.args[0]).__name__ for call in mock_fire.call_args_list]
            # Warnings present, so EndOfRunSummary should fire
            assert "EndOfRunSummary" in fired_types
            assert "StatsLine" in fired_types
            assert "FreshnessCheckComplete" in fired_types

    def test_task_end_messages_some_errors(self, args, config, manifest):
        """Test that EndOfRunSummary fires when there are errors"""
        from dbt.artifacts.schemas.freshness import FreshnessStatus

        task = FreshnessTask(args=args, config=config, manifest=manifest)
        results = [
            self._create_result(FreshnessStatus.Pass, "source1", "table1"),
            self._create_result(FreshnessStatus.Error, "source2", "table2"),
        ]
        with mock.patch("dbt.task.freshness.fire_event") as mock_fire:
            task.task_end_messages(results)
            fired_types = [type(call.args[0]).__name__ for call in mock_fire.call_args_list]
            # Errors present, so EndOfRunSummary should fire
            assert "EndOfRunSummary" in fired_types
            assert "StatsLine" in fired_types
            assert "FreshnessCheckComplete" in fired_types

    def test_task_end_messages_mixed_results(self, args, config, manifest):
        """Test counts are accurate when there are mixed pass, warn, and error results"""
        from dbt.artifacts.schemas.freshness import FreshnessStatus

        task = FreshnessTask(args=args, config=config, manifest=manifest)
        results = [
            self._create_result(FreshnessStatus.Pass, "source1", "table1"),
            self._create_result(FreshnessStatus.Warn, "source2", "table2"),
            self._create_result(FreshnessStatus.Error, "source3", "table3"),
            self._create_result(FreshnessStatus.RuntimeErr, "source4", "table4"),
        ]
        with mock.patch("dbt.task.freshness.fire_event") as mock_fire:
            task.task_end_messages(results)
            fired_types = [type(call.args[0]).__name__ for call in mock_fire.call_args_list]
            assert "EndOfRunSummary" in fired_types
            assert "StatsLine" in fired_types
            assert "FreshnessCheckComplete" in fired_types
            # Verify error and warning counts are correct
            summary_calls = [
                c
                for c in mock_fire.call_args_list
                if type(c.args[0]).__name__ == "EndOfRunSummary"
            ]
            assert summary_calls[0].args[0].num_errors == 2
            assert summary_calls[0].args[0].num_warnings == 1

    def test_task_end_messages_empty_results(self, args, config, manifest):
        """Test that FreshnessCheckComplete still fires with empty results"""
        task = FreshnessTask(args=args, config=config, manifest=manifest)
        with mock.patch("dbt.task.freshness.fire_event") as mock_fire:
            task.task_end_messages([])
            fired_types = [type(call.args[0]).__name__ for call in mock_fire.call_args_list]
            # FreshnessCheckComplete should always fire even with no results
            assert "FreshnessCheckComplete" in fired_types
            assert "EndOfRunSummary" not in fired_types
