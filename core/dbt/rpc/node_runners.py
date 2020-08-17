from abc import abstractmethod
from typing import Generic, TypeVar

import dbt.exceptions
from dbt.contracts.rpc import (
    RemoteCompileResult, RemoteRunResult, ResultTable,
)
from dbt.logger import GLOBAL_LOGGER as logger
from dbt.task.compile import CompileRunner
from dbt.rpc.error import dbt_error, RPCException, server_error


RPCSQLResult = TypeVar('RPCSQLResult', bound=RemoteCompileResult)


class GenericRPCRunner(CompileRunner, Generic[RPCSQLResult]):
    def __init__(self, config, adapter, node, node_index, num_nodes):
        CompileRunner.__init__(
            self, config, adapter, node, node_index, num_nodes
        )

    def handle_exception(self, e, ctx):
        logger.debug('Got an exception: {}'.format(e), exc_info=True)
        if isinstance(e, dbt.exceptions.Exception):
            if isinstance(e, dbt.exceptions.RuntimeException):
                e.add_node(ctx.node)
            return dbt_error(e)
        elif isinstance(e, RPCException):
            return e
        else:
            return server_error(e)

    def before_execute(self):
        pass

    def after_execute(self, result):
        pass

    def compile(self, manifest):
        compiler = self.adapter.get_compiler()
        return compiler.compile_node(self.node, manifest, {}, write=False)

    @abstractmethod
    def execute(self, compiled_node, manifest) -> RPCSQLResult:
        pass

    @abstractmethod
    def from_run_result(self, result, start_time, timing_info) -> RPCSQLResult:
        pass

    def error_result(self, node, error, start_time, timing_info):
        raise error

    def ephemeral_result(self, node, start_time, timing_info):
        raise dbt.exceptions.NotImplementedException(
            'cannot execute ephemeral nodes remotely!'
        )


class RPCCompileRunner(GenericRPCRunner[RemoteCompileResult]):
    def execute(self, compiled_node, manifest) -> RemoteCompileResult:
        return RemoteCompileResult(
            raw_sql=compiled_node.raw_sql,
            compiled_sql=compiled_node.injected_sql,
            node=compiled_node,
            timing=[],  # this will get added later
            logs=[],
        )

    def from_run_result(
        self, result, start_time, timing_info
    ) -> RemoteCompileResult:
        return RemoteCompileResult(
            raw_sql=result.raw_sql,
            compiled_sql=result.compiled_sql,
            node=result.node,
            timing=timing_info,
            logs=[],
        )


class RPCExecuteRunner(GenericRPCRunner[RemoteRunResult]):
    def execute(self, compiled_node, manifest) -> RemoteRunResult:
        _, execute_result = self.adapter.execute(
            compiled_node.injected_sql, fetch=True
        )

        table = ResultTable(
            column_names=list(execute_result.column_names),
            rows=[list(row) for row in execute_result],
        )

        return RemoteRunResult(
            raw_sql=compiled_node.raw_sql,
            compiled_sql=compiled_node.injected_sql,
            node=compiled_node,
            table=table,
            timing=[],
            logs=[],
        )

    def from_run_result(
        self, result, start_time, timing_info
    ) -> RemoteRunResult:
        return RemoteRunResult(
            raw_sql=result.raw_sql,
            compiled_sql=result.compiled_sql,
            node=result.node,
            table=result.table,
            timing=timing_info,
            logs=[],
        )
