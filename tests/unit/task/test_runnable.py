from argparse import Namespace
from unittest.mock import MagicMock, patch

import pytest

from dbt.artifacts.schemas.results import NodeResult, NodeStatus
from dbt.exceptions import DbtInternalError, DbtRuntimeError, FailFastError
from dbt.flags import get_flags, set_from_args
from dbt.task.run import RunTask
from dbt.task.runnable import GraphRunnableTask
from tests.unit.utils.manifest import (
    make_exposure,
    make_manifest,
    make_model,
    make_source,
)


def make_task(manifest=None) -> RunTask:
    return RunTask(get_flags(), None, manifest)


def _node_result(status: NodeStatus) -> NodeResult:
    node = MagicMock()
    node.unique_id = "model.pkg.foo"
    result = MagicMock(spec=NodeResult)
    result.status = status
    result.node = node
    result.message = "error message"
    return result


class TestResolveNode:
    def test_finds_node_in_nodes_collection(self):
        model = make_model("pkg", "my_model", "select 1")
        task = make_task(make_manifest(nodes=[model]))
        assert task._resolve_node(model.unique_id) is model

    def test_finds_node_in_sources_collection(self):
        source = make_source("pkg", "my_source", "my_table")
        task = make_task(make_manifest(sources=[source]))
        assert task._resolve_node(source.unique_id) is source

    def test_finds_node_in_exposures_collection(self):
        exposure = make_exposure("pkg", "my_exposure")
        task = make_task(make_manifest(exposures=[exposure]))
        assert task._resolve_node(exposure.unique_id) is exposure

    def test_raises_when_uid_not_in_any_collection(self):
        task = make_task(make_manifest())
        with pytest.raises(DbtInternalError, match="unknown.uid"):
            task._resolve_node("unknown.uid")

    def test_nodes_collection_checked_before_sources(self):
        # A uid present only in nodes is returned from nodes (not missed).
        model = make_model("pkg", "my_model", "select 1")
        task = make_task(make_manifest(nodes=[model]))
        result = task._resolve_node(model.unique_id)
        assert result.unique_id == model.unique_id


class TestNoExplicitSelection:
    def test_returns_true_when_no_select_or_exclude(self):
        task = make_task()
        task.args = MagicMock(select=None, exclude=None)
        assert task._no_explicit_selection() is True

    def test_returns_false_when_select_is_set(self):
        task = make_task()
        task.args = MagicMock(select=("my_model",), exclude=None)
        assert task._no_explicit_selection() is False

    def test_returns_false_when_exclude_is_set(self):
        task = make_task()
        task.args = MagicMock(select=None, exclude=("my_model",))
        assert task._no_explicit_selection() is False

    def test_returns_false_when_both_select_and_exclude_set(self):
        task = make_task()
        task.args = MagicMock(select=("model_a",), exclude=("model_b",))
        assert task._no_explicit_selection() is False


class TestCachableManifestNodes:
    def test_includes_regular_relational_model(self):
        model = make_model("pkg", "my_model", "select 1")
        task = make_task(make_manifest(nodes=[model]))
        assert model in task._cachable_manifest_nodes()

    def test_excludes_ephemeral_model(self):
        ephemeral = make_model(
            "pkg", "ephemeral_model", "select 1", config_kwargs={"materialized": "ephemeral"}
        )
        task = make_task(make_manifest(nodes=[ephemeral]))
        assert ephemeral not in task._cachable_manifest_nodes()

    def test_excludes_external_node(self):
        # is_external_node returns True when both path and original_file_path are empty.
        external = make_model("pkg", "external_model", "select 1")
        external.path = ""
        external.original_file_path = ""
        task = make_task(make_manifest(nodes=[external]))
        assert external not in task._cachable_manifest_nodes()

    def test_excludes_non_relational_nodes(self):
        # GenericTestNode has resource_type=NodeType.Test, not in REFABLE_NODE_TYPES,
        # so is_relational returns False and it must be excluded from the cache.
        from tests.unit.utils.manifest import make_generic_test

        model = make_model("pkg", "m", "select 1")
        test_node = make_generic_test("pkg", "not_null", model, {})
        task = make_task(make_manifest(nodes=[test_node]))
        assert test_node not in task._cachable_manifest_nodes()

    def test_raises_when_manifest_is_none(self):
        task = make_task(manifest=None)
        with pytest.raises(DbtInternalError):
            task._cachable_manifest_nodes()

    def test_returns_multiple_matching_nodes(self):
        model_a = make_model("pkg", "model_a", "select 1")
        model_b = make_model("pkg", "model_b", "select 2")
        ephemeral = make_model(
            "pkg", "ephemeral", "select 3", config_kwargs={"materialized": "ephemeral"}
        )
        task = make_task(make_manifest(nodes=[model_a, model_b, ephemeral]))
        cachable = task._cachable_manifest_nodes()
        assert model_a in cachable
        assert model_b in cachable
        assert ephemeral not in cachable


class TestListSchemasForDb:
    def test_returns_lowercased_db_schema_tuples(self):
        adapter = MagicMock()
        adapter.list_schemas.return_value = ["MySchema", "OtherSchema"]
        db_only = MagicMock()
        db_only.database = "MyDb"
        db_only.__str__ = lambda self: "MyDb"

        result = GraphRunnableTask._list_schemas_for_db(adapter, db_only)

        assert result == [("mydb", "myschema"), ("mydb", "otherschema")]

    def test_filters_out_none_schemas(self):
        adapter = MagicMock()
        adapter.list_schemas.return_value = ["ValidSchema", None, "AnotherSchema"]
        db_only = MagicMock()
        db_only.database = "db"
        db_only.__str__ = lambda self: "db"

        result = GraphRunnableTask._list_schemas_for_db(adapter, db_only)

        assert len(result) == 2
        assert all(s is not None for _, s in result)

    def test_handles_none_database(self):
        adapter = MagicMock()
        adapter.list_schemas.return_value = ["my_schema"]
        db_only = MagicMock()
        db_only.database = None

        result = GraphRunnableTask._list_schemas_for_db(adapter, db_only)

        assert result == [(None, "my_schema")]
        # When database is None, list_schemas is called with None
        adapter.list_schemas.assert_called_once_with(None)


class TestCreateSchemaForRelation:
    def _make_adapter(self):
        adapter = MagicMock()
        adapter.connection_named.return_value.__enter__ = MagicMock(return_value=None)
        adapter.connection_named.return_value.__exit__ = MagicMock(return_value=False)
        return adapter

    def test_creates_schema_via_named_connection(self):
        adapter = self._make_adapter()
        relation = MagicMock(database="mydb", schema="myschema")

        GraphRunnableTask._create_schema_for_relation(adapter, relation)

        adapter.connection_named.assert_called_once_with("create_mydb_myschema")
        adapter.create_schema.assert_called_once_with(relation)

    def test_connection_name_uses_empty_string_for_none_database(self):
        adapter = self._make_adapter()
        relation = MagicMock(database=None, schema="myschema")

        GraphRunnableTask._create_schema_for_relation(adapter, relation)

        adapter.connection_named.assert_called_once_with("create__myschema")


class TestCheckFailFastOrRaise:
    def test_sets_fail_fast_error_on_error_status_with_fail_fast_flag(self):
        set_from_args(Namespace(fail_fast=True), {})
        task = make_task()
        result = _node_result(NodeStatus.Error)

        task._check_fail_fast_or_raise(result)

        assert isinstance(task._raise_next_tick, FailFastError)

    def test_sets_fail_fast_error_on_fail_status_with_fail_fast_flag(self):
        set_from_args(Namespace(fail_fast=True), {})
        task = make_task()
        result = _node_result(NodeStatus.Fail)

        task._check_fail_fast_or_raise(result)

        assert isinstance(task._raise_next_tick, FailFastError)

    def test_sets_fail_fast_error_on_partial_success_with_fail_fast_flag(self):
        set_from_args(Namespace(fail_fast=True), {})
        task = make_task()
        result = _node_result(NodeStatus.PartialSuccess)

        task._check_fail_fast_or_raise(result)

        assert isinstance(task._raise_next_tick, FailFastError)

    def test_sets_runtime_error_on_error_when_raise_on_first_error(self):
        task = make_task()
        result = _node_result(NodeStatus.Error)
        with patch.object(task, "raise_on_first_error", return_value=True):
            task._check_fail_fast_or_raise(result)

        assert isinstance(task._raise_next_tick, DbtRuntimeError)

    def test_no_error_set_on_success(self):
        task = make_task()
        result = _node_result(NodeStatus.Success)

        task._check_fail_fast_or_raise(result)

        assert task._raise_next_tick is None

    def test_fail_fast_takes_precedence_over_raise_on_first_error(self):
        set_from_args(Namespace(fail_fast=True), {})
        task = make_task()
        result = _node_result(NodeStatus.Error)
        with patch.object(task, "raise_on_first_error", return_value=True):
            task._check_fail_fast_or_raise(result)

        assert isinstance(task._raise_next_tick, FailFastError)


class TestHandleFailFastSkips:
    def _make_node_result(self, uid: str) -> NodeResult:
        result = MagicMock(spec=NodeResult)
        result.node = MagicMock(unique_id=uid)
        result.status = NodeStatus.Success
        return result

    def _make_failure(self):
        failure = MagicMock(spec=FailFastError)
        failure.result = MagicMock()
        return failure

    def test_marks_unexecuted_nodes_as_skipped(self):
        model_a = make_model("pkg", "model_a", "select 1")
        model_b = make_model("pkg", "model_b", "select 2")
        task = make_task()
        task._flattened_nodes = [model_a, model_b]
        task.node_results = []

        with patch("dbt.task.runnable.print_run_result_error"):
            results = task._handle_fail_fast_skips(self._make_failure())

        skipped_ids = {r.node.unique_id for r in results}
        assert model_a.unique_id in skipped_ids
        assert model_b.unique_id in skipped_ids

    def test_does_not_skip_already_executed_nodes(self):
        model_a = make_model("pkg", "model_a", "select 1")
        model_b = make_model("pkg", "model_b", "select 2")
        task = make_task()
        task._flattened_nodes = [model_a, model_b]
        task.node_results = [self._make_node_result(model_a.unique_id)]

        with patch("dbt.task.runnable.print_run_result_error"):
            results = task._handle_fail_fast_skips(self._make_failure())

        result_ids = [r.node.unique_id for r in results]
        assert result_ids.count(model_a.unique_id) == 1

    def test_returns_node_results_list(self):
        task = make_task()
        task._flattened_nodes = []
        task.node_results = []

        with patch("dbt.task.runnable.print_run_result_error"):
            result = task._handle_fail_fast_skips(self._make_failure())

        assert result is task.node_results


class TestSubmitCreateSchemaFutures:
    def _make_relation(self, database, schema):
        rel = MagicMock()
        rel.database = database
        rel.schema = schema
        return rel

    def test_submits_future_for_new_schema(self):
        task = make_task()
        tpe = MagicMock()
        adapter = MagicMock()
        rel = self._make_relation("mydb", "myschema")
        existing: set = set()

        task._submit_create_schema_futures(tpe, adapter, {rel}, existing)

        tpe.submit_connected.assert_called_once()

    def test_skips_schema_already_in_existing(self):
        task = make_task()
        tpe = MagicMock()
        adapter = MagicMock()
        rel = self._make_relation("MyDb", "MySchema")
        # Pre-populate with the lowercased version
        existing = {("mydb", "myschema")}

        task._submit_create_schema_futures(tpe, adapter, {rel}, existing)

        tpe.submit_connected.assert_not_called()

    def test_adds_submitted_schema_to_existing(self):
        task = make_task()
        tpe = MagicMock()
        adapter = MagicMock()
        rel = self._make_relation("mydb", "myschema")
        existing: set = set()

        task._submit_create_schema_futures(tpe, adapter, {rel}, existing)

        assert ("mydb", "myschema") in existing

    def test_skips_relation_with_none_schema(self):
        task = make_task()
        tpe = MagicMock()
        adapter = MagicMock()
        rel = self._make_relation("mydb", None)

        task._submit_create_schema_futures(tpe, adapter, {rel}, set())

        tpe.submit_connected.assert_not_called()
