import signal
import threading
from dataclasses import dataclass
from datetime import datetime
from typing import Union, List, Optional

from hologram import JsonSchemaMixin

from dbt.adapters.factory import get_adapter
from dbt.clients.jinja import extract_toplevel_blocks
from dbt.compilation import compile_manifest
from dbt.contracts.results import RemoteCatalogResults
from dbt.parser.results import ParseResult
from dbt.parser.rpc import RPCCallParser, RPCMacroParser
from dbt.parser.util import ParserUtils
import dbt.ui.printer
from dbt.logger import GLOBAL_LOGGER as logger
from dbt.rpc.node_runners import RPCCompileRunner, RPCExecuteRunner
from dbt.rpc.task import RemoteCallableResult, RPCTask

from dbt.task.generate import GenerateTask
from dbt.task.run import RunTask
from dbt.task.seed import SeedTask
from dbt.task.test import TestTask


@dataclass
class RPCExecParameters(JsonSchemaMixin):
    name: str
    sql: str
    macros: Optional[str]


@dataclass
class RPCCompileProjectParameters(JsonSchemaMixin):
    models: Union[None, str, List[str]] = None
    exclude: Union[None, str, List[str]] = None


@dataclass
class RPCTestProjectParameters(RPCCompileProjectParameters):
    data: bool = False
    schema: bool = False


@dataclass
class RPCSeedProjectParameters(JsonSchemaMixin):
    show: bool = False


@dataclass
class RPCDocsGenerateProjectParameters(JsonSchemaMixin):
    compile: bool = True


class _RPCExecTask(RPCTask):
    def __init__(self, args, config, manifest):
        super().__init__(args, config)
        self._base_manifest = manifest.deepcopy(config=config)

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

    def _get_exec_node(self, name, sql, macros):
        results = ParseResult.rpc()
        macro_overrides = {}
        sql, macros = self._extract_request_data(sql)

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
        node = rpc_parser.parse_remote(sql, name)
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

    def handle_request(
        self, params: RPCExecParameters
    ) -> RemoteCallableResult:
        # we could get a ctrl+c at any time, including during parsing.
        thread = None
        started = datetime.utcnow()
        try:
            node = self._get_exec_node(params.name, params.sql, params.macros)

            selected_uids = [node.unique_id]
            self.runtime_cleanup(selected_uids)

            thread_done = threading.Event()
            thread = threading.Thread(target=self._in_thread,
                                      args=(node, thread_done))
            thread.start()
            thread_done.wait()
        except KeyboardInterrupt:
            adapter = get_adapter(self.config)
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

    def get_runner_type(self):
        return RPCCompileRunner


class RemoteRunTask(_RPCExecTask, RunTask):
    METHOD_NAME = 'run_sql'

    def get_runner_type(self):
        return RPCExecuteRunner


class RemoteCompileProjectTask(RPCTask):
    METHOD_NAME = 'compile'

    def __init__(self, args, config, manifest):
        super().__init__(args, config)
        self.manifest = manifest.deepcopy(config=config)

    def load_manifest(self):
        # we started out with a manifest!
        pass

    def handle_request(
        self, params: RPCCompileProjectParameters
    ) -> RemoteCallableResult:
        self.args.models = self._listify(params.models)
        self.args.exclude = self._listify(params.exclude)

        results = self.run()
        return results


class RemoteRunProjectTask(RPCTask, RunTask):
    METHOD_NAME = 'run'

    def __init__(self, args, config, manifest):
        super().__init__(args, config)
        self.manifest = manifest.deepcopy(config=config)

    def load_manifest(self):
        # we started out with a manifest!
        pass

    def handle_request(
        self, params: RPCCompileProjectParameters
    ) -> RemoteCallableResult:
        self.args.models = self._listify(params.models)
        self.args.exclude = self._listify(params.exclude)

        results = self.run()
        return results


class RemoteSeedProjectTask(RPCTask, SeedTask):
    METHOD_NAME = 'seed'

    def __init__(self, args, config, manifest):
        super().__init__(args, config)
        self.manifest = manifest.deepcopy(config=config)

    def load_manifest(self):
        # we started out with a manifest!
        pass

    def handle_request(
        self, params: RPCSeedProjectParameters
    ) -> RemoteCallableResult:
        self.args.show = params.show

        results = self.run()
        return results


class RemoteTestProjectTask(RPCTask, TestTask):
    METHOD_NAME = 'test'

    def __init__(self, args, config, manifest):
        super().__init__(args, config)
        self.manifest = manifest.deepcopy(config=config)

    def load_manifest(self):
        # we started out with a manifest!
        pass

    def handle_request(
        self, params: RPCTestProjectParameters,
    ) -> RemoteCallableResult:
        self.args.models = self._listify(params.models)
        self.args.exclude = self._listify(params.exclude)
        self.args.data = params.data
        self.args.schema = params.schema

        results = self.run()
        return results


class RemoteDocsGenerateProjectTask(RPCTask, GenerateTask):
    METHOD_NAME = 'docs.generate'

    def __init__(self, args, config, manifest):
        super().__init__(args, config)
        self.manifest = manifest.deepcopy(config=config)

    def load_manifest(self):
        # we started out with a manifest!
        pass

    def handle_request(
        self, params: RPCDocsGenerateProjectParameters,
    ) -> RemoteCallableResult:
        self.args.models = None
        self.args.exclude = None
        self.args.compile = params.compile

        results = self.run()
        assert isinstance(results, RemoteCatalogResults)
        return results

    def get_catalog_results(
        self, nodes, generated_at, compile_results
    ) -> RemoteCatalogResults:
        return RemoteCatalogResults(
            nodes=nodes,
            generated_at=datetime.utcnow(),
            _compile_results=compile_results,
            logs=[],
        )
