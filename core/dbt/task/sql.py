import time
import traceback
from abc import ABCMeta, abstractmethod
from contextlib import nullcontext
from datetime import datetime, timezone
from typing import Generic, List, TypeVar

import dbt_common.exceptions.base
from dbt.adapters.base.impl import BaseAdapter
from dbt.artifacts.schemas.results import RunningStatus, TimingInfo, collect_timing_info
from dbt.compilation import Compiler
from dbt.config import RuntimeConfig
from dbt.contracts.graph.manifest import Manifest
from dbt.contracts.sql import (
    RemoteCompileResult,
    RemoteCompileResultMixin,
    RemoteRunResult,
    ResultTable,
)
from dbt.events.types import (
    NodeCompiling,
    NodeConnectionReleaseError,
    NodeExecuting,
    SQLRunnerException,
)
from dbt.flags import get_flags
from dbt.task.base import ExecutionContext
from dbt_common.events.functions import fire_event

SQLResult = TypeVar("SQLResult", bound=RemoteCompileResultMixin)


class GenericSqlRunner(Generic[SQLResult], metaclass=ABCMeta):
    """Standalone runner for programmatic SQL execution (compile/execute).

    Intentionally NOT a subclass of BaseRunner — its result type
    (RemoteCompileResultMixin) lives in a separate hierarchy from NodeResult/RunResult
    and is used for programmatic SQL execution, not DAG running.
    """

    def __init__(
        self,
        config: RuntimeConfig,
        adapter: BaseAdapter,
        node,
        node_index: int,
        num_nodes: int,
    ) -> None:
        self.config = config
        self.compiler = Compiler(config)
        self.adapter = adapter
        self.node = node
        self.node_index = node_index
        self.num_nodes = num_nodes

    def compile(self, manifest: Manifest):
        return self.compiler.compile_node(self.node, manifest, {}, write=False)

    def before_execute(self) -> None:
        pass

    def after_execute(self, result: SQLResult) -> None:
        pass

    def handle_exception(self, e: Exception, ctx: ExecutionContext) -> Exception:
        fire_event(
            SQLRunnerException(
                exc=str(e), exc_info=traceback.format_exc(), node_info=self.node.node_info
            )
        )
        return e

    @abstractmethod
    def execute(self, compiled_node, manifest: Manifest) -> SQLResult:
        pass

    @abstractmethod
    def from_run_result(
        self, result: SQLResult, start_time: float, timing_info: List[TimingInfo]
    ) -> SQLResult:
        pass

    def error_result(
        self, node, error: Exception, start_time: float, timing_info: List[TimingInfo]
    ) -> SQLResult:
        raise error

    def ephemeral_result(
        self, node, start_time: float, timing_info: List[TimingInfo]
    ) -> SQLResult:
        raise dbt_common.exceptions.base.NotImplementedError(
            "cannot execute ephemeral nodes remotely!"
        )

    def _safe_release_connection(self) -> None:
        try:
            self.adapter.release_connection()
        except Exception as exc:
            fire_event(
                NodeConnectionReleaseError(
                    node_name=self.node.name, exc=str(exc), exc_info=traceback.format_exc()
                )
            )

    def safe_run(self, manifest: Manifest) -> SQLResult:
        started = time.time()
        ctx = ExecutionContext(self.node)
        error = None
        result = None

        try:
            with (
                self.adapter.connection_named(self.node.unique_id, self.node)
                if get_flags().INTROSPECT
                else nullcontext()
            ):
                ctx.node.update_event_status(node_status=RunningStatus.Compiling)
                fire_event(NodeCompiling(node_info=ctx.node.node_info))
                with collect_timing_info("compile", ctx.timing.append):
                    ctx.node = self.compile(manifest)

                ctx.node.update_event_status(node_status=RunningStatus.Executing)
                fire_event(NodeExecuting(node_info=ctx.node.node_info))
                with collect_timing_info("execute", ctx.timing.append):
                    result = self.execute(ctx.node, manifest)
        except Exception as e:
            error = self.handle_exception(e, ctx)
        finally:
            self._safe_release_connection()

        if error is not None:
            return self.error_result(ctx.node, error, started, ctx.timing)
        assert result is not None
        return self.from_run_result(result, started, ctx.timing)


class SqlCompileRunner(GenericSqlRunner[RemoteCompileResult]):
    def execute(self, compiled_node, manifest: Manifest) -> RemoteCompileResult:
        return RemoteCompileResult(
            raw_code=compiled_node.raw_code,
            compiled_code=compiled_node.compiled_code,
            node=compiled_node,
            timing=[],  # this will get added later
            generated_at=datetime.now(timezone.utc).replace(tzinfo=None),
        )

    def from_run_result(
        self, result: RemoteCompileResult, start_time: float, timing_info: List[TimingInfo]
    ) -> RemoteCompileResult:
        return RemoteCompileResult(
            raw_code=result.raw_code,
            compiled_code=result.compiled_code,
            node=result.node,
            timing=timing_info,
            generated_at=datetime.now(timezone.utc).replace(tzinfo=None),
        )


class SqlExecuteRunner(GenericSqlRunner[RemoteRunResult]):
    def execute(self, compiled_node, manifest: Manifest) -> RemoteRunResult:
        _, execute_result = self.adapter.execute(compiled_node.compiled_code, fetch=True)

        table = ResultTable(
            column_names=list(execute_result.column_names),
            rows=[list(row) for row in execute_result],
        )

        return RemoteRunResult(
            raw_code=compiled_node.raw_code,
            compiled_code=compiled_node.compiled_code,
            node=compiled_node,
            table=table,
            timing=[],
            generated_at=datetime.now(timezone.utc).replace(tzinfo=None),
        )

    def from_run_result(
        self, result: RemoteRunResult, start_time: float, timing_info: List[TimingInfo]
    ) -> RemoteRunResult:
        return RemoteRunResult(
            raw_code=result.raw_code,
            compiled_code=result.compiled_code,
            node=result.node,
            table=result.table,
            timing=timing_info,
            generated_at=datetime.now(timezone.utc).replace(tzinfo=None),
        )
