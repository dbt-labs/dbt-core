import os

from dbt.adapters.factory import get_adapter
from dbt.clients.jinja import extract_toplevel_blocks
from dbt.compilation import compile_manifest
from dbt.loader import load_all_projects, GraphLoader
from dbt.node_runners import CompileRunner, RPCCompileRunner
from dbt.node_types import NodeType
from dbt.parser.analysis import RPCCallParser
from dbt.parser.macros import MacroParser
from dbt.parser.util import ParserUtils
import dbt.ui.printer

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

    def __init__(self, args, config):
        super(RemoteCompileTask, self).__init__(args, config)
        self._base_manifest = GraphLoader.load_all(
            config,
            internal_manifest=get_adapter(config).check_internal_manifest()
        )

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
            if block.render():
                data_chunks.append(block.contents)
            if block.block_type_name == 'macro':
                macro_blocks.append(block.full_block)
        macros = '\n'.join(macro_blocks)
        sql = ''.join(data_chunks)
        return sql, macros

    def handle_request(self, name, sql):
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
        selected_uids = [node.unique_id]
        self.runtime_cleanup(selected_uids)
        self.job_queue = self.linker.as_graph_queue(self.manifest,
                                                    selected_uids)

        result = self.get_runner(node).safe_run(self.manifest)

        return result.serialize()
