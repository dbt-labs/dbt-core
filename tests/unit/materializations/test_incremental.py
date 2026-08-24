from types import SimpleNamespace
from unittest.mock import MagicMock, call

from dbt.materializations.incremental.executor import (
    IncrementalMaterializationExecutionState,
    IncrementalMaterializationExecutor,
)


def _relation(name, relation_type="table", *, is_view=False):
    relation = MagicMock(name=name)
    relation.name = name
    relation.type = relation_type
    relation.is_view = is_view
    relation.include.return_value = f'"analytics"."{name}"'
    return relation


def _context(existing_relation):
    this = MagicMock(name="this")
    target = _relation("target")
    intermediate = _relation("intermediate")
    backup = _relation("backup")
    temp = _relation("temp")
    this.incorporate.return_value = target
    context = {
        "adapter": MagicMock(),
        "this": this,
        "config": {
            "unique_key": "id",
            "incremental_strategy": "delete+insert",
            "tmp_relation_type": "view",
            "on_schema_change": "append_new_columns",
            "grants": {"select": ["reporter"]},
            "predicates": ["id > 0"],
            "contract": {"enforced": False},
        },
        "model": {"unique_id": "model.test.orders"},
        "sql": "select 1 as id",
        "pre_hooks": ["pre"],
        "post_hooks": ["post"],
        "load_cached_relation": MagicMock(side_effect=[existing_relation, None, None]),
        "make_intermediate_relation": MagicMock(return_value=intermediate),
        "make_backup_relation": MagicMock(return_value=backup),
        "make_temp_relation": MagicMock(return_value=temp),
        "should_full_refresh": MagicMock(return_value=False),
        "incremental_validate_on_schema_change": MagicMock(return_value="append_new_columns"),
        "drop_relation_if_exists": MagicMock(),
        "run_hooks": MagicMock(),
        "process_schema_changes": MagicMock(return_value=["id"]),
        "process_config_changes": MagicMock(),
        "apply_config_changeset": MagicMock(),
        "write": MagicMock(),
        "store_result": MagicMock(),
        "create_indexes": MagicMock(),
        "should_revoke": MagicMock(return_value=False),
        "apply_grants": MagicMock(),
        "create_table_structure_at": MagicMock(),
        "apply_alter_constraints": MagicMock(),
        "apply_tags": MagicMock(),
        "apply_column_tags": MagicMock(),
        "insert_from_relation": MagicMock(),
        "set_overwrite_mode": MagicMock(),
        "persist_docs": MagicMock(),
    }
    return context, target, intermediate, backup, temp


def _model():
    return SimpleNamespace(language="sql")


def _operation(kind, **kwargs):
    return SimpleNamespace(kind=SimpleNamespace(value=kind), **kwargs)


def test_incremental_executor_plans_before_constructing_typed_staging_relation():
    existing = _relation("existing")
    context, target, intermediate, backup, temp = _context(existing)
    typed_temp = _relation("typed_temp", "view")
    temp.incorporate.return_value = typed_temp
    incremental_plan = SimpleNamespace(
        temp_relation_type=SimpleNamespace(value="view"),
        catalog_staging=SimpleNamespace(value="permanent_table_only"),
    )
    strategy_macro = MagicMock()
    catalog_relation = object()
    adapter = MagicMock()
    adapter.build_catalog_relation.return_value = catalog_relation
    adapter.plan_incremental_mutation.return_value = incremental_plan
    adapter.get_incremental_plan_macro.return_value = strategy_macro

    state = IncrementalMaterializationExecutor(
        adapter, _model(), context
    ).resolve_incremental_execution_state()

    adapter.plan_incremental_mutation.assert_called_once_with(
        "delete+insert",
        language="sql",
        unique_key="id",
        requested_temp_relation_type="view",
        catalog_relation=catalog_relation,
    )
    temp.incorporate.assert_called_once_with(type="view")
    assert state.temp_relation is typed_temp
    assert state.staging_is_temporary is False
    assert state.incremental_plan is incremental_plan
    assert state.strategy_macro is strategy_macro
    assert state.target_relation is target
    assert state.intermediate_relation is intermediate
    assert state.backup_relation is backup


def test_incremental_executor_builds_staging_then_mutation_from_typed_arguments(
    monkeypatch,
):
    existing = _relation("existing")
    context, target, intermediate, backup, temp = _context(existing)
    strategy_macro = MagicMock(return_value="delete from target; insert into target")
    state = IncrementalMaterializationExecutionState(
        existing_relation=existing,
        target_relation=target,
        intermediate_relation=intermediate,
        backup_relation=backup,
        temp_relation=temp,
        preexisting_intermediate_relation=None,
        preexisting_backup_relation=None,
        incremental_plan=SimpleNamespace(),
        strategy_macro=strategy_macro,
        catalog_relation=object(),
        unique_key="id",
        staging_is_temporary=True,
        full_refresh_mode=False,
        on_schema_change="append_new_columns",
        grant_config={},
    )
    strategy_arguments = MagicMock()
    strategy_arguments.to_macro_dict.return_value = {"target_relation": target}
    adapter = MagicMock()
    adapter.execute.return_value = (object(), object())
    adapter.plan_incremental_arguments.return_value = strategy_arguments
    executor = IncrementalMaterializationExecutor(adapter, _model(), context)
    monkeypatch.setattr(executor, "_build_sql", MagicMock(return_value="create temp view"))

    build_sql = executor._incremental_mutation_sql(state)

    assert build_sql == "delete from target; insert into target"
    executor._build_sql.assert_called_once_with(temp, temporary=True)
    adapter.execute.assert_called_once_with("create temp view", auto_begin=True, fetch=False)
    adapter.expand_target_column_types.assert_called_once_with(
        from_relation=temp,
        to_relation=target,
    )
    adapter.plan_incremental_arguments.assert_called_once_with(
        target_relation=target,
        temp_relation=temp,
        unique_key="id",
        dest_columns=["id"],
        incremental_predicates=["id > 0"],
        adapter_arguments={
            "catalog_relation": state.catalog_relation,
            "incremental_plan": state.incremental_plan,
        },
    )
    strategy_macro.assert_called_once_with({"target_relation": target})


def test_incremental_overwrite_mode_uses_planned_runtime_engine():
    existing = _relation("existing")
    context, target, intermediate, backup, temp = _context(existing)
    state = IncrementalMaterializationExecutionState(
        existing_relation=existing,
        target_relation=target,
        intermediate_relation=intermediate,
        backup_relation=backup,
        temp_relation=temp,
        preexisting_intermediate_relation=None,
        preexisting_backup_relation=None,
        incremental_plan=SimpleNamespace(
            facts=SimpleNamespace(
                runtime=SimpleNamespace(engine="databricks_sql_warehouse")
            )
        ),
        strategy_macro=MagicMock(),
        catalog_relation=None,
        unique_key="id",
        staging_is_temporary=True,
        full_refresh_mode=False,
        on_schema_change="ignore",
        grant_config={},
    )
    executor = IncrementalMaterializationExecutor(MagicMock(), _model(), context)

    executor._execute_incremental_program(
        state,
        (_operation("set_incremental_overwrite_mode", name="DYNAMIC"),),
    )

    context["set_overwrite_mode"].assert_called_once_with(
        "DYNAMIC", "databricks_sql_warehouse"
    )


def test_incremental_executor_runs_python_lifecycle_and_mutation(monkeypatch):
    existing = _relation("existing")
    context, target, intermediate, backup, temp = _context(existing)
    state = IncrementalMaterializationExecutionState(
        existing_relation=existing,
        target_relation=target,
        intermediate_relation=intermediate,
        backup_relation=backup,
        temp_relation=temp,
        preexisting_intermediate_relation=None,
        preexisting_backup_relation=None,
        incremental_plan=SimpleNamespace(),
        strategy_macro=MagicMock(),
        catalog_relation=None,
        unique_key="id",
        staging_is_temporary=True,
        full_refresh_mode=False,
        on_schema_change="ignore",
        grant_config={"select": ["reporter"]},
    )
    adapter = MagicMock()
    executor = IncrementalMaterializationExecutor(adapter, _model(), context)
    monkeypatch.setattr(
        executor,
        "resolve_incremental_execution_state",
        MagicMock(return_value=state),
    )
    monkeypatch.setattr(
        executor,
        "_incremental_mutation_sql",
        MagicMock(return_value="merge into target"),
    )
    monkeypatch.setattr(executor, "_execute_main", MagicMock())

    result = executor.execute()

    assert result == {"relations": [target]}
    executor._incremental_mutation_sql.assert_called_once_with(state)
    executor._execute_main.assert_called_once_with("merge into target")
    context["create_indexes"].assert_not_called()
    context["run_hooks"].assert_has_calls(
        [
            call(context["pre_hooks"], inside_transaction=False),
            call(context["pre_hooks"], inside_transaction=True),
            call(context["post_hooks"], inside_transaction=True),
            call(context["post_hooks"], inside_transaction=False),
        ]
    )
    context["apply_grants"].assert_called_once_with(
        target,
        {"select": ["reporter"]},
        should_revoke=False,
    )
    context["adapter"].commit.assert_called_once_with()
    adapter.rename_relation.assert_not_called()
    adapter.drop_relation.assert_not_called()


def test_incremental_executor_full_refresh_swaps_and_drops_backup(monkeypatch):
    existing = _relation("existing")
    context, target, intermediate, backup, temp = _context(existing)
    state = IncrementalMaterializationExecutionState(
        existing_relation=existing,
        target_relation=target,
        intermediate_relation=intermediate,
        backup_relation=backup,
        temp_relation=temp,
        preexisting_intermediate_relation=None,
        preexisting_backup_relation=None,
        incremental_plan=SimpleNamespace(),
        strategy_macro=MagicMock(),
        catalog_relation=None,
        unique_key="id",
        staging_is_temporary=True,
        full_refresh_mode=True,
        on_schema_change="ignore",
        grant_config={},
    )
    adapter = MagicMock()
    executor = IncrementalMaterializationExecutor(adapter, _model(), context)
    monkeypatch.setattr(
        executor,
        "resolve_incremental_execution_state",
        MagicMock(return_value=state),
    )
    monkeypatch.setattr(executor, "_build_sql", MagicMock(return_value="create intermediate"))
    monkeypatch.setattr(executor, "_execute_main", MagicMock())

    executor.execute()

    executor._build_sql.assert_called_once_with(intermediate)
    context["create_indexes"].assert_called_once_with(intermediate)
    assert adapter.rename_relation.call_args_list == [
        call(target, backup),
        call(intermediate, target),
    ]
    adapter.drop_relation.assert_called_once_with(backup)


def test_incremental_executor_uses_ordered_mutation_program(monkeypatch):
    existing = _relation("existing")
    context, target, intermediate, backup, temp = _context(existing)
    strategy_macro = MagicMock(return_value=["delete from target", "insert into target"])
    operations = (
        _operation(
            "capture_config_changes",
            relation=SimpleNamespace(value="existing"),
        ),
        _operation(
            "create_from_query",
            relation=SimpleNamespace(value="temp"),
            temporary=True,
            auto_begin=True,
        ),
        _operation(
            "expand_target_column_types",
            relation=SimpleNamespace(value="target"),
            source=SimpleNamespace(value="temp"),
        ),
        _operation(
            "process_schema_changes",
            relation=SimpleNamespace(value="existing"),
            source=SimpleNamespace(value="temp"),
        ),
        _operation(
            "process_config_changes",
            relation=SimpleNamespace(value="target"),
            source=SimpleNamespace(value="existing"),
        ),
        _operation(
            "execute_incremental_mutation",
            relation=SimpleNamespace(value="target"),
            source=SimpleNamespace(value="temp"),
        ),
        _operation(
            "apply_config_changes",
            relation=SimpleNamespace(value="target"),
            source=SimpleNamespace(value="existing"),
        ),
        _operation(
            "apply_grants",
            relation=SimpleNamespace(value="target"),
        ),
        _operation(
            "persist_documentation",
            relation=SimpleNamespace(value="target"),
        ),
        _operation("commit"),
    )
    lifecycle_plan = SimpleNamespace(
        operations=operations,
        schema_change=SimpleNamespace(strategy=SimpleNamespace(value="append_new_columns")),
        partition=SimpleNamespace(
            to_dict=MagicMock(
                return_value={
                    "field": "event_date",
                    "data_type": "date",
                    "copy_partitions": False,
                }
            )
        ),
    )
    state = IncrementalMaterializationExecutionState(
        existing_relation=existing,
        target_relation=target,
        intermediate_relation=intermediate,
        backup_relation=backup,
        temp_relation=temp,
        preexisting_intermediate_relation=None,
        preexisting_backup_relation=None,
        incremental_plan=SimpleNamespace(),
        strategy_macro=strategy_macro,
        catalog_relation=None,
        unique_key="id",
        staging_is_temporary=True,
        full_refresh_mode=False,
        on_schema_change="append_new_columns",
        grant_config={"select": ["reporter"]},
        lifecycle_plan=lifecycle_plan,
    )
    strategy_arguments = MagicMock()
    strategy_arguments.to_macro_dict.return_value = {"target_relation": target}
    adapter = MagicMock()
    adapter.execute.return_value = (object(), object())
    adapter.plan_incremental_arguments.return_value = strategy_arguments
    model_config = MagicMock()
    configuration_changes = object()
    model_config.get_changeset.return_value = configuration_changes
    adapter.get_config_from_model.return_value = model_config
    adapter.get_relation_config.return_value = object()
    executor = IncrementalMaterializationExecutor(adapter, _model(), context)
    monkeypatch.setattr(
        executor,
        "resolve_incremental_execution_state",
        MagicMock(return_value=state),
    )
    monkeypatch.setattr(executor, "_build_sql", MagicMock(return_value="create temp view"))

    result = executor.execute()

    assert result == {"relations": [target]}
    assert adapter.execute.call_args_list[0] == call(
        "create temp view", auto_begin=True, fetch=False
    )
    assert adapter.execute.call_args_list[1:] == [
        call("delete from target", auto_begin=True, fetch=False),
        call("insert into target", auto_begin=True, fetch=False),
    ]
    context["process_config_changes"].assert_called_once_with(target, existing)
    context["apply_config_changeset"].assert_called_once_with(
        target,
        context["model"],
        configuration_changes,
        existing,
    )
    adapter.expand_target_column_types.assert_called_once_with(
        from_relation=temp,
        to_relation=target,
    )
    context["process_schema_changes"].assert_called_once_with("append_new_columns", temp, existing)
    strategy_macro.assert_called_once_with({"target_relation": target})
    adapter.plan_incremental_arguments.assert_called_once_with(
        target_relation=target,
        temp_relation=temp,
        unique_key="id",
        dest_columns=["id"],
        incremental_predicates=["id > 0"],
        adapter_arguments={
            "catalog_relation": None,
            "incremental_plan": state.incremental_plan,
            "partition_plan": {
                "field": "event_date",
                "data_type": "date",
                "copy_partitions": False,
            },
        },
    )
    context["apply_grants"].assert_called_once_with(
        target,
        {"select": ["reporter"]},
        should_revoke=False,
    )
    context["adapter"].commit.assert_called_once_with()


def test_incremental_executor_expands_typed_create_and_populate_operations():
    existing = _relation("existing")
    context, target, intermediate, backup, temp = _context(existing)
    enriched = _relation("enriched_target")
    context["create_table_structure_at"].return_value = enriched
    column_tags = SimpleNamespace(set_column_tags={"id": "sensitive"})
    operations = (
        _operation(
            "create_structure_from_relation",
            relation=SimpleNamespace(value="target"),
            source=SimpleNamespace(value="intermediate"),
        ),
        _operation(
            "apply_alter_constraints",
            relation=SimpleNamespace(value="target"),
        ),
        _operation("apply_tags", relation=SimpleNamespace(value="target")),
        _operation("apply_column_tags", relation=SimpleNamespace(value="target")),
        _operation(
            "insert_from_relation",
            relation=SimpleNamespace(value="target"),
            source=SimpleNamespace(value="intermediate"),
        ),
    )
    lifecycle_plan = SimpleNamespace(
        operations=operations,
        schema_change=SimpleNamespace(strategy=SimpleNamespace(value="ignore")),
        partition=None,
        facts=SimpleNamespace(
                create=SimpleNamespace(
                    catalog=SimpleNamespace(
                        to_dict=lambda: {"catalog_type": "unity"},
                    ),
                    format=SimpleNamespace(
                    to_dict=lambda: {"table_provider": "iceberg"},
                )
            )
        ),
    )
    state = IncrementalMaterializationExecutionState(
        existing_relation=existing,
        target_relation=target,
        intermediate_relation=intermediate,
        backup_relation=backup,
        temp_relation=temp,
        preexisting_intermediate_relation=None,
        preexisting_backup_relation=None,
        incremental_plan=SimpleNamespace(),
        strategy_macro=MagicMock(),
        catalog_relation=None,
        unique_key="id",
        staging_is_temporary=True,
        full_refresh_mode=True,
        on_schema_change="ignore",
        grant_config={},
        lifecycle_plan=lifecycle_plan,
    )
    adapter = MagicMock()
    adapter.get_column_tags_from_model.return_value = column_tags
    executor = IncrementalMaterializationExecutor(adapter, _model(), context)

    executor._execute_incremental_program(state, operations)

    context["create_table_structure_at"].assert_called_once_with(
        target,
        intermediate,
            context["sql"],
            {"table_provider": "iceberg"},
            {"catalog_type": "unity"},
        )
    context["apply_alter_constraints"].assert_called_once_with(enriched)
    context["apply_tags"].assert_called_once_with(enriched, None)
    context["apply_column_tags"].assert_called_once_with(enriched, column_tags)
    context["insert_from_relation"].assert_called_once_with(enriched, intermediate)


def test_incremental_executor_dispatches_typed_partition_copy(monkeypatch):
    existing = _relation("existing")
    context, target, intermediate, backup, temp = _context(existing)
    partition = object()
    lifecycle_plan = SimpleNamespace(
        operations=(
            _operation(
                "copy_incremental_partitions",
                relation=SimpleNamespace(value="target"),
                source=SimpleNamespace(value="temp"),
            ),
        ),
        schema_change=SimpleNamespace(strategy=SimpleNamespace(value="ignore")),
        partition=partition,
    )
    state = IncrementalMaterializationExecutionState(
        existing_relation=existing,
        target_relation=target,
        intermediate_relation=intermediate,
        backup_relation=backup,
        temp_relation=temp,
        preexisting_intermediate_relation=None,
        preexisting_backup_relation=None,
        incremental_plan=SimpleNamespace(),
        strategy_macro=MagicMock(),
        catalog_relation=None,
        unique_key="id",
        staging_is_temporary=True,
        full_refresh_mode=False,
        on_schema_change="ignore",
        grant_config={},
        lifecycle_plan=lifecycle_plan,
    )
    adapter = MagicMock()
    executor = IncrementalMaterializationExecutor(adapter, _model(), context)
    monkeypatch.setattr(
        executor,
        "resolve_incremental_execution_state",
        MagicMock(return_value=state),
    )

    executor.execute()

    adapter.execute_incremental_partition_copy.assert_called_once_with(temp, target, partition)


def test_incremental_executor_dispatches_typed_insert_from_query(monkeypatch):
    existing = _relation("existing")
    context, target, intermediate, backup, temp = _context(existing)
    partition = object()
    lifecycle_plan = SimpleNamespace(
        operations=(
            _operation(
                "insert_from_query",
                relation=SimpleNamespace(value="temp"),
            ),
        ),
        schema_change=SimpleNamespace(strategy=SimpleNamespace(value="ignore")),
        partition=partition,
    )
    state = IncrementalMaterializationExecutionState(
        existing_relation=existing,
        target_relation=target,
        intermediate_relation=intermediate,
        backup_relation=backup,
        temp_relation=temp,
        preexisting_intermediate_relation=None,
        preexisting_backup_relation=None,
        incremental_plan=SimpleNamespace(),
        strategy_macro=MagicMock(),
        catalog_relation=None,
        unique_key="id",
        staging_is_temporary=True,
        full_refresh_mode=False,
        on_schema_change="ignore",
        grant_config={},
        lifecycle_plan=lifecycle_plan,
    )
    adapter = MagicMock()
    adapter.render_incremental_insert_from_query.return_value = "insert into temp select 1"
    adapter.execute.return_value = (object(), object())
    executor = IncrementalMaterializationExecutor(adapter, _model(), context)
    monkeypatch.setattr(
        executor,
        "resolve_incremental_execution_state",
        MagicMock(return_value=state),
    )

    executor.execute()

    adapter.render_incremental_insert_from_query.assert_called_once_with(
        temp,
        context["sql"],
        partition,
        None,
    )
    adapter.execute.assert_called_once_with(
        "insert into temp select 1", auto_begin=False, fetch=False
    )
