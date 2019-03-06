import os

from dbt.adapters.factory import get_adapter
from dbt.compilation import compile_manifest
from dbt.loader import load_all_projects, GraphLoader
from dbt.node_runners import CompileRunner, RPCCompileRunner
from dbt.node_types import NodeType
from dbt.parser.analysis import RPCCallParser
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
        self.parser = None
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

    def handle_request(self, name, sql):
        self.parser = RPCCallParser(
            self.config,
            all_projects=load_all_projects(self.config),
            macro_manifest=self._base_manifest
        )

        sql = self.decode_sql(sql)
        request_path = os.path.join(self.config.target_path, 'rpc', name)
        node_dict = {
            'name': name,
            'root_path': request_path,
            'resource_type': NodeType.RPCCall,
            'path': name + '.sql',
            'original_file_path': 'from remote system',
            'package_name': self.config.project_name,
            'raw_sql': sql,
        }
        unique_id, node = self.parser.parse_sql_node(node_dict)

        self.manifest = ParserUtils.add_new_refs(
            manifest=self._base_manifest,
            current_project=self.config,
            node=node
        )
        # don't write our new, weird manifest!
        self.linker = compile_manifest(self.config, self.manifest, write=False)
        selected_uids = [node.unique_id]
        self.runtime_cleanup(selected_uids)
        self.job_queue = self.linker.as_graph_queue(self.manifest,
                                                    selected_uids)

        result = self.get_runner(node).safe_run(self.manifest)

        return result.serialize()
