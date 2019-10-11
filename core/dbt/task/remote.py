import shlex
import signal
import threading
from datetime import datetime
from typing import Type

import dbt.exceptions
import dbt.ui.printer
from dbt.adapters.factory import get_adapter
from dbt.clients.jinja import extract_toplevel_blocks
from dbt.compilation import compile_manifest
from dbt.contracts.rpc import (
    RPCExecParameters,
    RPCCompileParameters,
    RPCTestParameters,
    RPCSeedParameters,
    RPCDocsGenerateParameters,
    RPCCliParameters,
    RemoteCatalogResults,
    RemoteExecutionResult,
)
from dbt.parser.results import ParseResult
from dbt.parser.rpc import RPCCallParser, RPCMacroParser
from dbt.parser.util import ParserUtils
from dbt.logger import GLOBAL_LOGGER as logger
from dbt.rpc.node_runners import (
    RPCCompileRunner, RPCExecuteRunner
)
from dbt.rpc.task import RPCTask, Parameters

from dbt.task.generate import GenerateTask
from dbt.task.run import RunTask
from dbt.task.seed import SeedTask
from dbt.task.test import TestTask


class _RPCExecTask(RPCTask[RPCExecParameters]):
    def runtime_cleanup(self, selected_uids):
        """Do some pre-run cleanup that is usually performed in Task __init__.
        """
        self.run_count = 0
        self.num_nodes = len(selected_uids)
        self.node_results = []
        self._skipped_children = {}
        self._skipped_children = {}
        self._raise_next_tick = None

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
        results = ParseResult.rpc()
        macro_overrides = {}
        macros = self.args.macros
        sql, macros = self._extract_request_data(self.args.sql)

        if macros:
            macro_parser = RPCMacroParser(results, self.config)
            for node in macro_parser.parse_remote(macros):
                macro_overrides[node.unique_id] = node

        self._base_manifest.macros.update(macro_overrides)
        rpc_parser = RPCCallParser(
            results=results,
            project=self.config,
            root_project=self.config,
            macro_manifest=self._base_manifest,
        )
        node = rpc_parser.parse_remote(sql, self.args.name)
        self.manifest = ParserUtils.add_new_refs(
            manifest=self._base_manifest,
            current_project=self.config,
            node=node,
            macros=macro_overrides
        )

        # don't write our new, weird manifest!
        self.linker = compile_manifest(self.config, self.manifest, write=False)
        return node

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

            raise dbt.exceptions.RPCKilledException(signal.SIGINT)

        self._raise_set_error()

        ended = datetime.utcnow()
        elapsed = (ended - started).total_seconds()
        return self.get_result(
            results=self.node_results,
            elapsed_time=elapsed,
            generated_at=ended,
        )


class RemoteCompileTask(_RPCExecTask):
    METHOD_NAME = 'compile_sql'

    def handle_request(self) -> RemoteExecutionResult:
        # TODO: annotate that this is a RemoteExecutionResult of
        # RemoteCompileResults.
        return super().handle_request()

    def get_runner_type(self):
        return RPCCompileRunner


class RemoteRunTask(_RPCExecTask, RunTask):
    METHOD_NAME = 'run_sql'

    def handle_request(self) -> RemoteExecutionResult:
        # TODO: annotate that this is a RemoteExecutionResult of
        # RemoteRunResult.
        return super().handle_request()

    def get_runner_type(self):
        return RPCExecuteRunner


class _RPCCommandTask(RPCTask[Parameters]):
    def __init__(self, args, config, manifest):
        super().__init__(args, config, manifest)
        self.manifest = self._base_manifest

    def load_manifest(self):
        # we started out with a manifest!
        pass

    def handle_request(self) -> RemoteExecutionResult:
        return self.run()


class RemoteCompileProjectTask(_RPCCommandTask[RPCCompileParameters]):
    METHOD_NAME = 'compile'

    def set_args(self, params: RPCCompileParameters) -> None:
        self.args.models = self._listify(params.models)
        self.args.exclude = self._listify(params.exclude)


class RemoteRunProjectTask(_RPCCommandTask[RPCCompileParameters], RunTask):
    METHOD_NAME = 'run'

    def set_args(self, params: RPCCompileParameters) -> None:
        self.args.models = self._listify(params.models)
        self.args.exclude = self._listify(params.exclude)


class RemoteSeedProjectTask(_RPCCommandTask[RPCSeedParameters], SeedTask):
    METHOD_NAME = 'seed'

    def set_args(self, params: RPCSeedParameters) -> None:
        self.args.show = params.show


class RemoteTestProjectTask(_RPCCommandTask[RPCTestParameters], TestTask):
    METHOD_NAME = 'test'

    def set_args(self, params: RPCTestParameters) -> None:
        self.args.models = self._listify(params.models)
        self.args.exclude = self._listify(params.exclude)
        self.args.data = params.data
        self.args.schema = params.schema


class RemoteDocsGenerateProjectTask(
    _RPCCommandTask[RPCDocsGenerateParameters],
    GenerateTask,
):
    METHOD_NAME = 'docs.generate'

    def set_args(self, params: RPCDocsGenerateParameters) -> None:
        self.args.models = None
        self.args.exclude = None
        self.args.compile = params.compile

    def get_catalog_results(
        self, nodes, generated_at, compile_results
    ) -> RemoteCatalogResults:
        return RemoteCatalogResults(
            nodes=nodes,
            generated_at=datetime.utcnow(),
            _compile_results=compile_results,
            logs=[],
        )


class RemoteRPCParameters(_RPCCommandTask[RPCCliParameters]):
    METHOD_NAME = 'cli_args'

    def set_args(self, params: RPCCliParameters) -> None:
        # more import cycles :(
        from dbt.main import parse_args, RPCArgumentParser
        split = shlex.split(params.cli)
        self.args = parse_args(split, RPCArgumentParser)

    def get_rpc_task_cls(self) -> Type[_RPCCommandTask]:
        # This is obnoxious, but we don't have actual access to the TaskManager
        # so instead we get to dig through all the subclasses of RPCTask
        # (recursively!) looking for a matching METHOD_NAME
        candidate: Type[_RPCCommandTask]
        for candidate in _RPCCommandTask.recursive_subclasses():
            if candidate.METHOD_NAME == self.args.rpc_method:
                return candidate
        # this shouldn't happen
        raise dbt.exceptions.InternalException(
            'No matching handler found for rpc method {} (which={})'
            .format(self.args.rpc_method, self.args.which)
        )

    def handle_request(self) -> RemoteExecutionResult:
        cls = self.get_rpc_task_cls()
        # we parsed args from the cli, so we're set on that front
        task = cls(self.args, self.config, self.manifest)
        return task.handle_request()
