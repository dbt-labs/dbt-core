import signal
import threading
from typing import Union, List, Dict, Any

from dbt.adapters.factory import get_adapter
from dbt.clients.jinja import extract_toplevel_blocks
from dbt.compilation import compile_manifest
from dbt.node_runners import CompileRunner, RPCCompileRunner
from dbt.node_types import NodeType
from dbt.parser.results import ParseResult
from dbt.parser.rpc import RPCCallParser, RPCMacroParser
from dbt.parser.util import ParserUtils
import dbt.ui.printer
from dbt.logger import GLOBAL_LOGGER as logger

from dbt.task.runnable import GraphRunnableTask, RemoteCallable


class CompileTask(GraphRunnableTask):
    def raise_on_first_error(self):
        return True

    def build_query(self):
        return {
            "include": self.args.models,
            "exclude": self.args.exclude,
            "resource_types": NodeType.executable(),
            "tags": [],
        }

    def get_runner_type(self):
        return CompileRunner

    def task_end_messages(self, results):
        dbt.ui.printer.print_timestamped_line('Done.')


class RemoteCompileTask(CompileTask, RemoteCallable):
    METHOD_NAME = 'compile'

    def __init__(self, args, config, manifest):
        super().__init__(args, config)
        self._base_manifest = manifest.deepcopy(config=config)

    def get_runner_type(self):
        return RPCCompileRunner

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

    def handle_request(self, name, sql, macros=None):
        # we could get a ctrl+c at any time, including during parsing.
        thread = None
        try:
            node = self._get_exec_node(name, sql, macros)

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
        return self.node_results[0].to_dict()


class RemoteCompileProjectTask(CompileTask, RemoteCallable):
    METHOD_NAME = 'compile_project'

    def __init__(self, args, config, manifest):
        super().__init__(args, config)
        self.manifest = manifest.deepcopy(config=config)

    def load_manifest(self):
        # we started out with a manifest!
        pass

    def handle_request(
        self,
        models: Union[None, str, List[str]] = None,
        exclude: Union[None, str, List[str]] = None,
    ) -> Dict[str, List[Any]]:
        self.args.models = self._listify(models)
        self.args.exclude = self._listify(exclude)

        results = self.run()
        return {'results': [r.to_dict() for r in results]}
