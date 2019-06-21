import os
import signal
import threading

from dbt.adapters.factory import get_adapter
from dbt.clients.jinja import extract_toplevel_blocks
from dbt.compilation import compile_manifest
from dbt.loader import load_all_projects
from dbt.node_runners import CompileRunner, RPCCompileRunner
from dbt.node_types import NodeType
from dbt.parser.analysis import RPCCallParser
from dbt.parser.macros import MacroParser
from dbt.parser.util import ParserUtils
import dbt.ui.printer
from dbt.logger import RPC_LOGGER as rpc_logger

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
        request_path = os.path.join(self.config.target_path, 'rpc', name)
        all_projects = load_all_projects(self.config)
        macro_overrides = {}
        sql, macros = self._extract_request_data(sql)

        if macros:
            macro_parser = MacroParser(self.config, all_projects)
            macro_overrides.update(macro_parser.parse_macro_file(
                macro_file_path='from remote system',
                macro_file_contents=macros,
                root_path=request_path,
                package_name=self.config.project_name,
                resource_type=NodeType.Macro
            ))

        self._base_manifest.macros.update(macro_overrides)
        rpc_parser = RPCCallParser(
            self.config,
            all_projects=all_projects,
            macro_manifest=self._base_manifest
        )

        node_dict = {
            'name': name,
            'root_path': request_path,
            'resource_type': NodeType.RPCCall,
            'path': name + '.sql',
            'original_file_path': 'from remote system',
            'package_name': self.config.project_name,
            'raw_sql': sql,
        }

        unique_id, node = rpc_parser.parse_sql_node(node_dict)
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
                    rpc_logger.debug('canceled query {}'.format(conn_name))
                if thread:
                    thread.join()
            else:
                msg = ("The {} adapter does not support query "
                       "cancellation. Some queries may still be "
                       "running!".format(adapter.type()))

                rpc_logger.debug(msg)

            raise dbt.exceptions.RPCKilledException(signal.SIGINT)

        self._raise_set_error()
        return self.node_results[0].serialize()
