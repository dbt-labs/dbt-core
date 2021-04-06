import base64
import signal
import threading
from datetime import datetime
from typing import Dict, Any

from dbt import flags
from dbt.adapters.factory import get_adapter
from dbt.clients.jinja import extract_toplevel_blocks
from dbt.config.runtime import RuntimeConfig
from dbt.contracts.graph.manifest import Manifest
from dbt.contracts.graph.parsed import ParsedRPCNode
from dbt.contracts.rpc import RPCExecParameters
from dbt.contracts.rpc import RemoteExecutionResult
from dbt.exceptions import RPCKilledException, InternalException
from dbt.logger import GLOBAL_LOGGER as logger
from dbt.parser.manifest import process_node, process_macro
from dbt.parser.rpc import RPCCallParser, RPCMacroParser
from dbt.rpc.error import invalid_params
from dbt.rpc.node_runners import RPCCompileRunner, RPCExecuteRunner
from dbt.task.compile import CompileTask
from dbt.task.run import RunTask

from .base import RPCTask


def add_new_refs(
    manifest: Manifest,
    config: RuntimeConfig,
    node: ParsedRPCNode,
    macros: Dict[str, Any]
) -> None:
    """Given a new node that is not in the manifest, insert the new node
    into it as if it were part of regular ref processing.
    """
    if config.args.single_threaded or flags.SINGLE_THREADED_HANDLER:
        manifest = manifest.deepcopy()
    # it's ok for macros to silently override a local project macro name
    manifest.macros.update(macros)

    for macro in macros.values():
        process_macro(config, manifest, macro)

    # We used to do 'manifest.add_nodes({node.unique_id: node}) here, but the
    # node has already been added to the Manifest by the RPCCallParser
    # now that we save nodes to the Manifest instead of ParseResults.
    process_node(config, manifest, node)


class RemoteRunSQLTask(RPCTask[RPCExecParameters]):
    def runtime_cleanup(self, selected_uids):
        """Do some pre-run cleanup that is usually performed in Task __init__.
        """
        self.run_count = 0
        self.num_nodes = len(selected_uids)
        self.node_results = []
        self._skipped_children = {}
        self._skipped_children = {}
        self._raise_next_tick = None

    def decode_sql(self, sql: str) -> str:
        """Base64 decode a string. This should only be used for sql in calls.

        :param str sql: The base64 encoded form of the original utf-8 string
        :return str: The decoded utf-8 string
        """
        # JSON is defined as using "unicode", we'll go a step further and
        # mandate utf-8 (though for the base64 part, it doesn't really matter!)
        base64_sql_bytes = str(sql).encode('utf-8')

        try:
            sql_bytes = base64.b64decode(base64_sql_bytes, validate=True)
        except ValueError:
            self.raise_invalid_base64(sql)

        return sql_bytes.decode('utf-8')

    @staticmethod
    def raise_invalid_base64(sql):
        raise invalid_params(
            data={
                'message': 'invalid base64-encoded sql input',
                'sql': str(sql),
            }
        )

    def _extract_request_data(self, data):
        data = self.decode_sql(data)
        macro_blocks = []
        data_chunks = []
        for block in extract_toplevel_blocks(data):
            if block.block_type_name == 'macro':
                macro_blocks.append(block.full_block)
            else:
                data_chunks.append(block.full_block)
        macros = '\n'.join(macro_blocks)
        sql = ''.join(data_chunks)
        return sql, macros

    def _get_exec_node(self):
        if self.manifest is None:
            raise InternalException(
                'manifest not set in _get_exec_node'
            )

        macro_overrides = {}
        macros = self.args.macros
        sql, macros = self._extract_request_data(self.args.sql)

        if macros:
            macro_parser = RPCMacroParser(self.config, self.manifest)
            for node in macro_parser.parse_remote(macros):
                macro_overrides[node.unique_id] = node

        self.manifest.macros.update(macro_overrides)
        rpc_parser = RPCCallParser(
            project=self.config,
            manifest=self.manifest,
            root_project=self.config,
        )
        rpc_node = rpc_parser.parse_remote(sql, self.args.name)
        add_new_refs(
            manifest=self.manifest,
            config=self.config,
            node=rpc_node,
            macros=macro_overrides
        )

        # don't write our new, weird manifest!
        adapter = get_adapter(self.config)
        compiler = adapter.get_compiler()
        self.graph = compiler.compile(self.manifest, write=False)
        # previously, this compiled the ancestors, but they are compiled at
        # runtime now.
        return rpc_node

    def _raise_set_error(self):
        if self._raise_next_tick is not None:
            raise self._raise_next_tick

    def _in_thread(self, node, thread_done):
        runner = self.get_runner(node)
        try:
            self.node_results.append(runner.safe_run(self.manifest))
        except Exception as exc:
            logger.debug('Got exception {}'.format(exc), exc_info=True)
            self._raise_next_tick = exc
        finally:
            thread_done.set()

    def set_args(self, params: RPCExecParameters):
        self.args.name = params.name
        self.args.sql = params.sql
        self.args.macros = params.macros

    def handle_request(self) -> RemoteExecutionResult:
        # we could get a ctrl+c at any time, including during parsing.
        thread = None
        started = datetime.utcnow()
        try:
            node = self._get_exec_node()

            selected_uids = [node.unique_id]
            self.runtime_cleanup(selected_uids)

            thread_done = threading.Event()
            thread = threading.Thread(target=self._in_thread,
                                      args=(node, thread_done))
            thread.start()
            thread_done.wait()
        except KeyboardInterrupt:
            adapter = get_adapter(self.config)  # type: ignore
            if adapter.is_cancelable():

                for conn_name in adapter.cancel_open_connections():
                    logger.debug('canceled query {}'.format(conn_name))
                if thread:
                    thread.join()
            else:
                msg = ("The {} adapter does not support query "
                       "cancellation. Some queries may still be "
                       "running!".format(adapter.type()))

                logger.debug(msg)

            raise RPCKilledException(signal.SIGINT)

        self._raise_set_error()

        ended = datetime.utcnow()
        elapsed = (ended - started).total_seconds()
        return self.get_result(
            results=self.node_results,
            elapsed_time=elapsed,
            generated_at=ended,
        )

    def interpret_results(self, results):
        return True


class RemoteCompileTask(RemoteRunSQLTask, CompileTask):
    METHOD_NAME = 'compile_sql'

    def get_runner_type(self):
        return RPCCompileRunner


class RemoteRunTask(RemoteRunSQLTask, RunTask):
    METHOD_NAME = 'run_sql'

    def get_runner_type(self):
        return RPCExecuteRunner
