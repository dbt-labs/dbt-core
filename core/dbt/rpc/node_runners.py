import dbt.exceptions
from dbt.compilation import compile_node
from dbt.contracts.results import (
    RemoteCompileResult, RemoteRunResult, ResultTable,
)
from dbt.logger import GLOBAL_LOGGER as logger
from dbt.node_runners import CompileRunner
from dbt.rpc.error import dbt_error, RPCException, server_error


class RPCCompileRunner(CompileRunner):
    def __init__(self, config, adapter, node, node_index, num_nodes):
        super().__init__(config, adapter, node, node_index, num_nodes)

    def handle_exception(self, e, ctx):
        logger.debug('Got an exception: {}'.format(e), exc_info=True)
        if isinstance(e, dbt.exceptions.Exception):
            if isinstance(e, dbt.exceptions.RuntimeException):
                e.node = ctx.node
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
        return compile_node(self.adapter, self.config, self.node, manifest, {},
                            write=False)

    def execute(self, compiled_node, manifest):
        return RemoteCompileResult(
            raw_sql=compiled_node.raw_sql,
            compiled_sql=compiled_node.injected_sql,
            node=compiled_node,
            timing=[],  # this will get added later
            logs=[],
        )

    def error_result(self, node, error, start_time, timing_info):
        raise error

    def ephemeral_result(self, node, start_time, timing_info):
        raise dbt.exceptions.NotImplementedException(
            'cannot execute ephemeral nodes remotely!'
        )

    def from_run_result(self, result, start_time, timing_info):
        return RemoteCompileResult(
            raw_sql=result.raw_sql,
            compiled_sql=result.compiled_sql,
            node=result.node,
            timing=timing_info,
            logs=[],
        )


class RPCExecuteRunner(RPCCompileRunner):
    def from_run_result(self, result, start_time, timing_info):
        return RemoteRunResult(
            raw_sql=result.raw_sql,
            compiled_sql=result.compiled_sql,
            node=result.node,
            table=result.table,
            timing=timing_info,
            logs=[],
        )

    def execute(self, compiled_node, manifest):
        status, table = self.adapter.execute(compiled_node.injected_sql,
                                             fetch=True)

        table = ResultTable(
            column_names=list(table.column_names),
            rows=[list(row) for row in table],
        )

        return RemoteRunResult(
            raw_sql=compiled_node.raw_sql,
            compiled_sql=compiled_node.injected_sql,
            node=compiled_node,
            table=table,
            timing=[],
            logs=[],
        )
