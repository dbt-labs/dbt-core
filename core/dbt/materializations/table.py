from dataclasses import dataclass
from importlib import import_module
from typing import Any, Dict, Mapping, Optional

from dbt.adapters.base.relation import BaseRelation
from dbt.contracts.graph.nodes import ModelNode
from dbt.exceptions import DbtInternalError


@dataclass(frozen=True)
class TableMaterializationExecutionState:
    """Late-bound relations and configuration for one table replacement."""

    existing_relation: Optional[BaseRelation]
    target_relation: BaseRelation
    intermediate_relation: BaseRelation
    backup_relation: BaseRelation
    preexisting_intermediate_relation: Optional[BaseRelation]
    preexisting_backup_relation: Optional[BaseRelation]
    grant_config: Mapping[str, Any]
    lifecycle_plan: Optional[Any]


class TableMaterializationExecutor:
    """Execute built-in table lifecycle in Python.

    Project and adapter macros remain callable at explicit compatibility boundaries,
    but control flow and database mutation ordering live here rather than in Jinja.
    """

    def __init__(
        self,
        adapter: Any,
        model: ModelNode,
        context: Dict[str, Any],
        lifecycle_plan: Optional[Any] = None,
    ) -> None:
        self.adapter = adapter
        self.model = model
        self.context = context
        self.lifecycle_plan = lifecycle_plan

    def _context_value(self, name: str) -> Any:
        try:
            return self.context[name]
        except KeyError:
            raise DbtInternalError(
                f"Python table materialization context is missing '{name}'"
            ) from None

    def _call_macro(self, name: str, *args: Any, **kwargs: Any) -> Any:
        macro = self._context_value(name)
        if not callable(macro):
            raise DbtInternalError(
                f"Python table materialization context value '{name}' is not callable"
            )
        return macro(*args, **kwargs)

    def resolve_execution_state(self) -> TableMaterializationExecutionState:
        existing_relation = self._call_macro("load_cached_relation", self._context_value("this"))
        resolve_target = getattr(self.adapter, "resolve_table_materialization_relation", None)
        target_relation = (
            resolve_target(self.model, self._context_value("this"))
            if callable(resolve_target)
            else self._context_value("this").incorporate(type="table")
        )
        lifecycle_plan = self.lifecycle_plan
        resolve_lifecycle = getattr(self.adapter, "resolve_table_lifecycle_plan", None)
        if lifecycle_plan is not None and callable(resolve_lifecycle):
            lifecycle_plan = resolve_lifecycle(
                lifecycle_plan,
                self.model,
                target_relation,
                existing_relation,
                self._context_value("config"),
            )
        intermediate_relation = self._call_macro("make_intermediate_relation", target_relation)
        backup_relation_type = "table" if existing_relation is None else existing_relation.type
        backup_relation = self._call_macro(
            "make_backup_relation", target_relation, backup_relation_type
        )

        grant_config = self._context_value("config").get("grants") or {}
        if not isinstance(grant_config, Mapping):
            raise DbtInternalError("Table materialization grants config must be a mapping")

        return TableMaterializationExecutionState(
            existing_relation=existing_relation,
            target_relation=target_relation,
            intermediate_relation=intermediate_relation,
            backup_relation=backup_relation,
            preexisting_intermediate_relation=self._call_macro(
                "load_cached_relation", intermediate_relation
            ),
            preexisting_backup_relation=self._call_macro("load_cached_relation", backup_relation),
            grant_config=grant_config,
            lifecycle_plan=lifecycle_plan,
        )

    def _execute_main(self, sql: str, *, auto_begin: bool = True) -> None:
        self._call_macro("write", sql)
        response, table = self.adapter.execute(sql, auto_begin=auto_begin, fetch=False)
        self._call_macro("store_result", "main", response=response, agate_table=table)

    def _commit(self) -> None:
        runtime_adapter = self._context_value("adapter")
        commit = getattr(runtime_adapter, "commit", None)
        if not callable(commit):
            raise DbtInternalError("Python materialization adapter boundary cannot commit")
        commit()

    @staticmethod
    def _macro_unique_id(macro: Any) -> Optional[str]:
        unique_id = getattr(getattr(macro, "macro", None), "unique_id", None)
        return unique_id if isinstance(unique_id, str) else None

    def _legacy_renderer_override(self, plan: Any) -> Optional[str]:
        context_macros = (
            ("get_create_table_as_sql", "macro.dbt.get_create_table_as_sql"),
            ("render_create_from_query_plan", "macro.dbt.render_create_from_query_plan"),
        )
        for macro_name, expected_unique_id in context_macros:
            selected_unique_id = self._macro_unique_id(self.context.get(macro_name))
            if selected_unique_id != expected_unique_id:
                return selected_unique_id or f"unresolved {macro_name} macro"

        adapter_wrapper = self.context.get("adapter")
        dispatch = getattr(adapter_wrapper, "dispatch", None)
        if not callable(dispatch):
            return "unresolved legacy renderer dispatch"

        dispatched_macros = (
            ("get_create_table_as_sql", "macro.dbt.default__get_create_table_as_sql"),
            ("render_create_from_query_plan", "macro.dbt.default__render_create_from_query_plan"),
            ("create_table_as", "macro.dbt.default__create_table_as"),
        )
        for macro_name, expected_unique_id in dispatched_macros:
            selected = dispatch(macro_name, "dbt")
            selected_unique_id = self._macro_unique_id(selected)
            if selected_unique_id != expected_unique_id:
                return selected_unique_id or f"unresolved {macro_name} dispatch"

        renderer_macro = getattr(plan, "renderer_macro", None)
        if not isinstance(renderer_macro, str):
            return "unresolved plan renderer"
        selected_renderer = self.context.get(renderer_macro)
        selected_renderer_unique_id = self._macro_unique_id(selected_renderer)
        expected_renderer_unique_id = f"macro.dbt.{renderer_macro}"
        if selected_renderer_unique_id != expected_renderer_unique_id:
            return selected_renderer_unique_id or f"unresolved {renderer_macro} renderer"
        return None

    @staticmethod
    def _render_arguments_type() -> Optional[Any]:
        """Load new adapter contract without raising core's adapter minimum version."""

        try:
            planning = import_module("dbt.adapters.planning")
        except ModuleNotFoundError as exc:
            if exc.name != "dbt.adapters.planning":
                raise
            return None
        return getattr(planning, "CreateFromQueryRenderArguments", None)

    def _legacy_build_sql(self, relation: BaseRelation, sql: str, temporary: bool = False) -> str:
        build_sql = self._call_macro("get_create_table_as_sql", temporary, relation, sql)
        if not isinstance(build_sql, str):
            raise DbtInternalError("Table create-from-query renderer must return SQL text")
        return build_sql

    def _build_sql(self, relation: BaseRelation, temporary: bool = False) -> str:
        sql = self._context_value("sql")
        render_arguments_type = self._render_arguments_type()
        resolve_render = getattr(self.adapter, "resolve_create_from_query_render", None)
        plan_create = getattr(self.adapter, "plan_create_from_query", None)
        if (
            render_arguments_type is None
            or not callable(resolve_render)
            or not callable(plan_create)
        ):
            return self._legacy_build_sql(relation, sql, temporary)

        create_plan = plan_create(temporary, relation, self.model)
        relation_sql = str(relation.include(database=not temporary, schema=not temporary))
        config = self._context_value("config")
        contract = config.get("contract")
        contract_enforced = (
            bool(contract.get("enforced"))
            if isinstance(contract, Mapping)
            else bool(getattr(contract, "enforced", False))
        )
        render_arguments = render_arguments_type(
            relation_sql=relation_sql,
            query=sql,
            sql_header=config.get("sql_header"),
            contract_enforced=contract_enforced,
            legacy_renderer_override=self._legacy_renderer_override(create_plan),
        )
        render_result = resolve_render(create_plan, render_arguments)
        render_kind = getattr(getattr(render_result, "kind", None), "value", None)
        if render_kind == "sql":
            rendered_sql = getattr(render_result, "sql", None)
            if not isinstance(rendered_sql, str):
                raise DbtInternalError("Typed create-from-query renderer returned invalid SQL")
            return rendered_sql
        if render_kind == "legacy_macro":
            renderer_macro = getattr(render_result, "renderer_macro", None)
            if renderer_macro != "get_create_table_as_sql":
                raise DbtInternalError(
                    "Typed create-from-query renderer selected an unknown compatibility macro"
                )
            return self._legacy_build_sql(relation, sql, temporary)
        raise DbtInternalError("Typed create-from-query renderer returned an unknown result kind")

    @staticmethod
    def _plan_value(plan: Optional[Any], name: str, default: str) -> str:
        value = getattr(plan, name, None)
        return getattr(value, "value", value) or default

    def _stage_lifecycle_values(self, plan: Optional[Any]) -> Dict[str, str]:
        values = {
            "replacement": self._plan_value(plan, "replacement", "stage_and_swap"),
            "hooks": self._plan_value(plan, "hooks", "split"),
            "indexes": self._plan_value(plan, "indexes", "before_swap"),
            "existing_indexes": self._plan_value(plan, "existing_indexes", "preserve"),
            "documentation": self._plan_value(plan, "documentation", "before_commit"),
            "transaction": self._plan_value(plan, "transaction", "explicit_commit"),
            "statement": self._plan_value(plan, "statement", "auto_begin"),
        }
        allowed = {
            "replacement": {"stage_and_swap"},
            "hooks": {"split", "in_transaction"},
            "indexes": {"before_swap", "after_swap", "none"},
            "existing_indexes": {"preserve", "drop_before_swap"},
            "documentation": {"before_commit", "after_commit"},
            "transaction": {"explicit_commit", "adapter_managed"},
            "statement": {"auto_begin", "no_auto_begin"},
        }
        for field_name, value in values.items():
            if value not in allowed[field_name]:
                raise DbtInternalError(
                    f"Table lifecycle planner selected unknown {field_name} policy '{value}'"
                )
        if (
            values["documentation"] == "after_commit"
            and values["transaction"] != "explicit_commit"
        ):
            raise DbtInternalError(
                "Post-commit table documentation requires explicit transaction control"
            )
        return values

    @staticmethod
    def _operation_value(operation: Any, name: str) -> Any:
        value = getattr(operation, name, None)
        return getattr(value, "value", value)

    def _execute_operation_program(
        self,
        *,
        operations: Any,
        relations: Mapping[str, Optional[BaseRelation]],
        existing_relation: Optional[BaseRelation],
        grant_config: Mapping[str, Any],
    ) -> None:
        """Execute a resolved, serializable materialization program in order."""

        envelope_context: Any = None
        setup_macro = getattr(self.lifecycle_plan, "setup_macro", None)
        teardown_macro = getattr(self.lifecycle_plan, "teardown_macro", None)

        for operation in operations:
            kind = self._operation_value(operation, "kind")
            relation_role = self._operation_value(operation, "relation")
            source_role = self._operation_value(operation, "source")
            destination_role = self._operation_value(operation, "destination")
            relation = relations.get(relation_role) if relation_role is not None else None

            if kind == "invoke_callback":
                name = getattr(operation, "name", None)
                if name == setup_macro:
                    envelope_context = self._call_macro(name)
                elif name == teardown_macro:
                    self._call_macro(name, envelope_context)
                elif relation_role is not None:
                    self._call_macro(name, relation)
                else:
                    self._call_macro(name)
            elif kind == "drop_relation_if_exists":
                self._call_macro("drop_relation_if_exists", relation)
            elif kind == "run_hooks":
                hook_name = getattr(operation, "name", None)
                hooks = self._context_value(f"{hook_name}_hooks")
                self._call_macro(
                    "run_hooks",
                    hooks,
                    inside_transaction=getattr(operation, "inside_transaction", None),
                )
            elif kind == "create_from_query":
                if relation is None:
                    raise DbtInternalError(
                        "Create-from-query operation resolved an empty relation"
                    )
                build_sql = self._build_sql(
                    relation,
                    temporary=bool(getattr(operation, "temporary", False)),
                )
                self._execute_main(
                    build_sql,
                    auto_begin=bool(getattr(operation, "auto_begin", False)),
                )
            elif kind == "create_from_relation":
                source = relations.get(source_role)
                if relation is None or source is None:
                    raise DbtInternalError(
                        "Create-from-relation operation resolved an empty relation"
                    )
                self._call_macro(
                    "create_table_at",
                    relation,
                    source,
                    self._context_value("sql"),
                )
            elif kind == "rename_relation":
                destination = relations.get(destination_role)
                if relation is None or destination is None:
                    raise DbtInternalError("Rename operation resolved an empty relation")
                self.adapter.rename_relation(relation, destination)
            elif kind == "create_indexes":
                self._call_macro("create_indexes", relation)
            elif kind == "apply_grants":
                should_revoke = self._call_macro(
                    "should_revoke",
                    existing_relation,
                    full_refresh_mode=True,
                )
                self._call_macro(
                    "apply_grants",
                    relation,
                    grant_config,
                    should_revoke=should_revoke,
                )
            elif kind == "apply_tags":
                self._call_macro(
                    "apply_tags",
                    relation,
                    self._context_value("config").get("databricks_tags"),
                )
            elif kind == "apply_column_tags":
                get_column_tags = getattr(self.adapter, "get_column_tags_from_model", None)
                if not callable(get_column_tags):
                    raise DbtInternalError(
                        "Column-tag operation requires an adapter column-tag resolver"
                    )
                column_tags = get_column_tags(self.model)
                if column_tags is not None and getattr(column_tags, "set_column_tags", None):
                    self._call_macro("apply_column_tags", relation, column_tags)
            elif kind == "persist_documentation":
                self._call_macro("persist_docs", relation, self._context_value("model"))
            elif kind == "persist_constraints":
                self._call_macro("persist_constraints", relation, self._context_value("model"))
            elif kind == "optimize":
                self._call_macro("optimize", relation)
            elif kind == "commit":
                self._commit()
            else:
                raise DbtInternalError(
                    f"Materialization program contains unknown operation '{kind}'"
                )

    def execute(self) -> Dict[str, Any]:
        state = self.resolve_execution_state()
        operations = getattr(state.lifecycle_plan, "operations", ())
        if operations:
            existing_relation = state.existing_relation
            if existing_relation is not None:
                existing_relation = self._call_macro("load_cached_relation", existing_relation)
            self._execute_operation_program(
                operations=operations,
                relations={
                    "existing": existing_relation,
                    "target": state.target_relation,
                    "intermediate": state.intermediate_relation,
                    "backup": state.backup_relation,
                },
                existing_relation=existing_relation,
                grant_config=state.grant_config,
            )
            return {"relations": [state.target_relation]}

        lifecycle = self._stage_lifecycle_values(state.lifecycle_plan)

        self._call_macro("drop_relation_if_exists", state.preexisting_intermediate_relation)
        self._call_macro("drop_relation_if_exists", state.preexisting_backup_relation)
        if lifecycle["hooks"] == "split":
            self._call_macro(
                "run_hooks", self._context_value("pre_hooks"), inside_transaction=False
            )
        self._call_macro("run_hooks", self._context_value("pre_hooks"), inside_transaction=True)

        build_sql = self._build_sql(state.intermediate_relation)
        self._execute_main(build_sql, auto_begin=lifecycle["statement"] == "auto_begin")
        if lifecycle["indexes"] == "before_swap":
            self._call_macro("create_indexes", state.intermediate_relation)

        existing_relation = state.existing_relation
        if existing_relation is not None:
            existing_relation = self._call_macro("load_cached_relation", existing_relation)
            if existing_relation is not None:
                if lifecycle["existing_indexes"] == "drop_before_swap":
                    self._call_macro("drop_indexes_on_relation", existing_relation)
                facts = getattr(state.lifecycle_plan, "facts", None)
                existing_facts = getattr(facts, "existing", None)
                can_be_renamed = getattr(existing_facts, "can_be_renamed", True)
                if can_be_renamed:
                    self.adapter.rename_relation(existing_relation, state.backup_relation)
                else:
                    self._call_macro("drop_relation_if_exists", existing_relation)

        self.adapter.rename_relation(state.intermediate_relation, state.target_relation)
        if lifecycle["indexes"] == "after_swap":
            self._call_macro("create_indexes", state.target_relation)
        self._call_macro("run_hooks", self._context_value("post_hooks"), inside_transaction=True)

        should_revoke = self._call_macro(
            "should_revoke", existing_relation, full_refresh_mode=True
        )
        self._call_macro(
            "apply_grants",
            state.target_relation,
            state.grant_config,
            should_revoke=should_revoke,
        )
        if lifecycle["documentation"] == "before_commit":
            self._call_macro("persist_docs", state.target_relation, self._context_value("model"))
        if lifecycle["transaction"] == "explicit_commit":
            self._commit()
        if lifecycle["documentation"] == "after_commit":
            self._call_macro("persist_docs", state.target_relation, self._context_value("model"))
            self._commit()
        self._call_macro("drop_relation_if_exists", state.backup_relation)
        if lifecycle["hooks"] == "split":
            self._call_macro(
                "run_hooks", self._context_value("post_hooks"), inside_transaction=False
            )

        return {"relations": [state.target_relation]}


@dataclass(frozen=True)
class DirectTableMaterializationExecutionState:
    """Late-bound relation and configuration for direct target replacement."""

    existing_relation: Optional[BaseRelation]
    target_relation: BaseRelation
    grant_config: Mapping[str, Any]


class DirectReplaceTableMaterializationExecutor(TableMaterializationExecutor):
    """Execute a typed direct-replacement table lifecycle in Python."""

    REQUIRED_ADAPTER_METHODS = (
        "plan_table_materialization",
        "resolve_table_materialization_existing_relation",
        "resolve_table_materialization_relation",
    )

    def resolve_direct_execution_state(self) -> DirectTableMaterializationExecutionState:
        existing_relation = self.adapter.resolve_table_materialization_existing_relation(
            self._context_value("this")
        )
        target_relation = self.adapter.resolve_table_materialization_relation(
            self.model, self._context_value("this")
        )
        resolve_lifecycle = getattr(self.adapter, "resolve_table_lifecycle_plan", None)
        if not callable(resolve_lifecycle):
            raise DbtInternalError("Direct table executor requires a runtime lifecycle resolver")
        self.lifecycle_plan = resolve_lifecycle(
            self.lifecycle_plan,
            self.model,
            target_relation,
            existing_relation,
            self._context_value("config"),
        )
        grant_config = self._context_value("config").get("grants") or {}
        if not isinstance(grant_config, Mapping):
            raise DbtInternalError("Table materialization grants config must be a mapping")
        return DirectTableMaterializationExecutionState(
            existing_relation=existing_relation,
            target_relation=target_relation,
            grant_config=grant_config,
        )

    def _direct_plan_value(self, name: str) -> Optional[str]:
        value = getattr(self.lifecycle_plan, name, None)
        return getattr(value, "value", value)

    def execute(self) -> Dict[str, Any]:
        if self._direct_plan_value("replacement") != "direct_replace":
            raise DbtInternalError(
                "Direct table executor requires a direct-replace lifecycle plan"
            )
        expected_policies = {
            "indexes": "none",
            "existing_indexes": "preserve",
            "documentation": "before_commit",
            "transaction": "adapter_managed",
            "hooks": "in_transaction",
        }
        for field_name, expected in expected_policies.items():
            if self._direct_plan_value(field_name) != expected:
                raise DbtInternalError(
                    f"Direct table executor requires {field_name} policy '{expected}'"
                )

        state = self.resolve_direct_execution_state()
        operations = getattr(self.lifecycle_plan, "operations", ())
        if operations:
            referenced_roles = {
                self._operation_value(operation, field_name)
                for operation in operations
                for field_name in ("relation", "source", "destination")
            }
            relations: Dict[str, Optional[BaseRelation]] = {
                "existing": state.existing_relation,
                "target": state.target_relation,
            }
            if "intermediate" in referenced_roles:
                relations["intermediate"] = self._call_macro(
                    "make_intermediate_relation", state.target_relation
                )
            if "staging" in referenced_roles:
                relations["staging"] = self._call_macro(
                    "make_staging_relation", state.target_relation
                )
            if "backup" in referenced_roles:
                backup_type = (
                    "table" if state.existing_relation is None else state.existing_relation.type
                )
                relations["backup"] = self._call_macro(
                    "make_backup_relation", state.target_relation, backup_type
                )
            self._execute_operation_program(
                operations=operations,
                relations=relations,
                existing_relation=state.existing_relation,
                grant_config=state.grant_config,
            )
            return {"relations": [state.target_relation]}

        setup_macro = getattr(self.lifecycle_plan, "setup_macro", None)
        teardown_macro = getattr(self.lifecycle_plan, "teardown_macro", None)
        envelope_context = self._call_macro(setup_macro) if setup_macro is not None else None

        self._call_macro("run_hooks", self._context_value("pre_hooks"), inside_transaction=True)

        facts = getattr(self.lifecycle_plan, "facts", None)
        existing_facts = getattr(facts, "existing", None)
        requires_drop = getattr(existing_facts, "requires_drop_before_replace", None)
        if requires_drop is None:
            needs_to_drop = getattr(state.target_relation, "needs_to_drop", None)
            requires_drop = bool(
                callable(needs_to_drop) and needs_to_drop(state.existing_relation)
            )
        if requires_drop:
            self._call_macro("drop_relation_if_exists", state.existing_relation)

        statement = self._direct_plan_value("statement")
        if statement not in {"auto_begin", "no_auto_begin"}:
            raise DbtInternalError(
                "Direct table executor requires a typed statement transaction policy"
            )
        self._execute_main(
            self._build_sql(state.target_relation),
            auto_begin=statement == "auto_begin",
        )
        self._call_macro("run_hooks", self._context_value("post_hooks"), inside_transaction=True)

        should_revoke = self._call_macro(
            "should_revoke", state.existing_relation, full_refresh_mode=True
        )
        self._call_macro(
            "apply_grants",
            state.target_relation,
            state.grant_config,
            should_revoke=should_revoke,
        )
        self._call_macro("persist_docs", state.target_relation, self._context_value("model"))

        if teardown_macro is not None:
            self._call_macro(teardown_macro, envelope_context)
        return {"relations": [state.target_relation]}
