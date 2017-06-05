
from dbt.logger import GLOBAL_LOGGER as logger
from dbt.exceptions import NotImplementedException
from dbt.utils import RunHookType, NodeType, get_nodes_by_tags

import dbt.utils
import dbt.tracking
import dbt.ui.printer

import time


def track_model_run(index, num_nodes, run_model_result):
    invocation_id = dbt.tracking.active_user.invocation_id
    dbt.tracking.track_model_run({
        "invocation_id": invocation_id,
        "index": index,
        "total": num_nodes,
        "execution_time": run_model_result.execution_time,
        "run_status": run_model_result.status,
        "run_skipped": run_model_result.skip,
        "run_error": run_model_result.error,
        "model_materialization": dbt.utils.get_materialization(run_model_result.node),  # noqa
        "model_id": dbt.utils.get_hash(run_model_result.node),
        "hashed_contents": dbt.utils.get_hashed_contents(run_model_result.node),  # noqa
    })


# TODO : push into runners!
def print_start_line(node, schema, index, total):
    if is_type(node, NodeType.Model):
        dbt.ui.printer.print_model_start_line(node, schema, index, total)
    if is_type(node, NodeType.Test):
        dbt.ui.printer.print_test_start_line(node, schema, index, total)
    if is_type(node, NodeType.Archive):
        dbt.ui.printer.print_archive_start_line(node, index, total)

class RunModelResult(object):
    def __init__(self, node, error=None, skip=False, status=None,
                 failed=None, execution_time=0):
        self.node = node
        self.error = error
        self.skip = skip
        self.fail = failed
        self.status = status
        self.execution_time = execution_time

    @property
    def errored(self):
        return self.error is not None

    @property
    def failed(self):
        return self.fail

    @property
    def skipped(self):
        return self.skip


class BaseRunner(object):
    Verb = ""

    def __init__(self, adapter, node, node_index, num_nodes):
        self.adapter = adapter
        self.node = node
        self.node_index = node_index
        self.num_nodes = num_nodes

        self.skip = False

    @classmethod
    def get_schema(cls, adapter, profile):
        return adapter.get_default_schema(profile)

    def before_model(self):
        pass

    def execute(self, project, flat_graph, existing):
        raise NotImplementedException()

    def after_model(self):
        pass

    def skip(self):
        self.skip = True

    @classmethod
    def before_run(self, project, adapter, flat_graph):
        pass

    @classmethod
    def after_run(self, project, adapter, results, elapsed):
        pass


class CompileRunner(BaseRunner):
    verb = "Compiling"

    @classmethod
    def compile_node(cls, project, node, flat_graph):
        compiler = dbt.compilation.Compiler(project)
        node = compiler.compile_node(node, flat_graph)
        return node

    def compile(self, project, adapter, node, flat_graph):
        result = RunModelResult(node)
        profile = project.run_environment()

        try:
            compiled_node = self.compile_node(project, node, flat_graph)
            result = RunModelResult(compiled_node)

        finally:
            adapter.release_connection(profile, node.get('name'))

        return result

    def execute(self, project, flat_graph, existing):
        return self.compile(project, self.adapter, self.node, flat_graph)


class ModelRunner(CompileRunner):
    verb = "Running"

    @classmethod
    def try_create_schema(cls, project, adapter):
        profile = project.run_environment()
        schema_name = cls.get_schema(adapter, profile)

        schema_exists = adapter.check_schema_exists(profile, schema_name)

        if schema_exists:
            logger.debug('schema {} already exists -- '
                         'not creating'.format(schema_name))
            return

        adapter.create_schema(profile, schema_name)

    @classmethod
    def run_hooks(cls, project, adapter, flat_graph, hook_type):
        profile = project.run_environment()

        nodes = flat_graph.get('nodes', {}).values()
        start_hooks = get_nodes_by_tags(nodes, {hook_type}, NodeType.Operation)
        hooks = [cls.compile_node(project, hook, flat_graph) for hook in start_hooks]

        for hook in start_hooks:
            compiled = cls.compile_node(project, hook, flat_graph)
            sql = compiled['wrapped_sql']
            adapter.execute_one(profile, sql, auto_begin=False)

    @classmethod
    def before_run(cls, project, adapter, flat_graph):
        cls.try_create_schema(project, adapter)
        cls.run_hooks(project, adapter, flat_graph, RunHookType.Start)

    @classmethod
    def print_results_line(results, execution_time):
        nodes = [r.node for r in results]
        stat_line = dbt.ui.printer.get_counts(nodes)

        dbt.ui.printer.print_timestamped_line("")
        dbt.ui.printer.print_timestamped_line(
            "Finished running {stat_line} in {execution_time:0.2f}s."
            .format(stat_line=stat_line, execution_time=execution_time))

    @classmethod
    def after_run(cls, project, adapter, results, elapsed):
        cls.run_hooks(project, adapter, flat_graph, RunHookType.End)
        cls.print_results_line(results, elapsed)

    def on_skip(self, project):
        profile = project.run_environment()
        schema_name = self.get_schema(self.adapter, profile)

        node_name = self.node.get('name')
        dbt.ui.printer.print_skip_line(self.node, schema_name, node_name,
                                       self.node_index, self.num_nodes)

        node_result = RunModelResult(self.node, skip=True)
        return node_result

    def execute_model(self, project, flat_graph, existing):
        start_time = time.time()

        profile = project.run_environment()
        schema_name = cls.get_schema(self.adapter, profile)
        is_ephemeral = (get_materialization(node) == 'ephemeral')

        if not is_ephemeral:
            print_start_line(self.node, schema_name, self.node_index,
                             self.num_nodes)

        node = self.compile_node(self.node, flat_graph)

        if not is_ephemeral:
            node, status = self.execute_node(self.node, flat_graph, existing,
                                             profile, self.adapter)

    def execute(self, project, flat_graph, existing):

        if self.skip:
            return self.on_skip()
        else:
            self.before_model(project)
            self.compile()
            run_model_result = self.execute_model(project, flat_graph, existing)
            self.after_model(project, run_model_result)

    def before_model(self, project):
        pass

    def after_model(self, project, result):
        track_model_run(self.node_index, self.num_nodes, result)


class TestRunner(CompileRunner):
    def execute(self, project, flat_graph, existing):
        self.compile()


class ArchiveRunner(CompileRunner):
    def execute(self, project, flat_graph, existing):
        self.compile()
