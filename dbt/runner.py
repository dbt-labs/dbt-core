import os
import time

from dbt.adapters.factory import get_adapter
from dbt.logger import GLOBAL_LOGGER as logger
from dbt.node_runners import RunModelResult

from dbt.utils import get_materialization, NodeType, is_type, get_nodes_by_tags

import dbt.clients.jinja
import dbt.compilation
import dbt.exceptions
import dbt.linker
import dbt.tracking
import dbt.model
import dbt.ui.printer

from  dbt.graph.selector import NodeSelector, FlatNodeSelector
from contextlib import contextmanager

from multiprocessing.dummy import Pool as ThreadPool


INTERNAL_ERROR_STRING = """This is an error in dbt. Please try again. If \
the error persists, open an issue at https://github.com/fishtown-analytics/dbt
""".strip()


@contextmanager
def model_error_handler(profile, adapter, node, run_model_result):
    catchable_errors = (dbt.exceptions.CompilationException,
                        dbt.exceptions.RuntimeException)

    try:
        yield

    except catchable_errors as e:
        run_model_result.error = str(e)
        run_model_result.status = 'ERROR'

    except dbt.exceptions.InternalException as e:
        build_path = node.get('build_path')
        prefix = 'Internal error executing {}'.format(build_path)

        error = "{prefix}\n{error}\n\n{note}".format(
                     prefix=dbt.ui.printer.red(prefix),
                     error=str(e).strip(),
                     note=INTERNAL_ERROR_STRING)
        logger.debug(error)

        run_model_result.error = str(e)
        run_model_result.status = 'ERROR'

    except Exception as e:
        prefix = "Unhandled error while executing {filepath}".format(
                    filepath=node.get('build_path'))

        error = "{prefix}\n{error}".format(
                     prefix=dbt.ui.printer.red(prefix),
                     error=str(e).strip())

        logger.debug(error)
        raise e

    finally:
        adapter.release_connection(profile, node.get('name'))


class RunManager(object):
    def __init__(self, project, target_path, args):
        self.project = project
        self.target_path = target_path
        self.args = args

        profile = self.project.run_environment()

        # TODO validate the number of threads
        if self.args.threads is None:
            self.threads = profile.get('threads', 1)
        else:
            self.threads = self.args.threads

    def deserialize_graph(self):
        logger.info("Loading dependency graph file.")

        base_target_path = self.project['target-path']
        graph_file = os.path.join(
            base_target_path,
            dbt.compilation.graph_file_name
        )

        return dbt.linker.from_file(graph_file)

    def safe_execute_node(self, data):
        node = data['node']
        flat_graph = data['flat_graph']
        existing = data['existing']
        schema_name = data['schema_name']
        node_index = data['node_index']
        num_nodes = data['num_nodes']

        start_time = time.time()

        error = None
        status = None
        is_ephemeral = (get_materialization(node) == 'ephemeral')

        if not is_ephemeral:
            print_start_line(node,
                             schema_name,
                             node_index,
                             num_nodes)

        profile = self.project.run_environment()
        adapter = get_adapter(profile)

        node = self.compile_node(node, flat_graph)

        if not is_ephemeral:
            node, status = self.execute_node(node, flat_graph, existing,
                                             profile, adapter)

        # ---

        execution_time = time.time() - start_time

        result = RunModelResult(node,
                                error=error,
                                status=status,
                                execution_time=execution_time)

        if not is_ephemeral:
            print_result_line(result, schema_name, node_index, num_nodes)

        return result

    def on_model_failure(self, linker, selected_nodes):
        def skip_dependent(node):
            dependent_nodes = linker.get_dependent_nodes(node.get('unique_id'))
            for node in dependent_nodes:
                if node in selected_nodes:
                    node_data = linker.get_node(node)
                    node_data['skip'] = True
                    linker.update_node_data(node, node_data)

        return skip_dependent

    def get_runners(self, Runner, adapter, node_dependency_list):
        all_nodes = dbt.utils.flatten_nodes(node_dependency_list)
        nodes = [n for n in all_nodes if get_materialization(n) != 'ephemeral']
        num_nodes = len(nodes)

        node_runners = {}
        for i, node in enumerate(nodes):
            uid = node.get('unique_id')
            runner = Runner(adapter, node, i + 1, num_nodes)
            node_runners[uid] = runner

        return node_runners

    def call_runner(self, data):
        runner = data['runner']
        existing = data['existing']
        flat_graph = data['flat_graph']

        node = runner.node
        adapter = runner.adapter
        profile = self.project.run_environment()
        # mtutable - this is set in context manager
        error_result = RunModelResult(node)

        result = None
        # TODO : what is this?
        with model_error_handler(profile, adapter, node, error_result):
            result = runner.execute(self.project, flat_graph, existing)

        if result is None:
            return error_result
        else:
            return result

    def execute_nodes(self, Runner, flat_graph, node_dependency_list):
        profile = self.project.run_environment()
        adapter = get_adapter(profile)
        schema_name = adapter.get_default_schema(profile)

        num_threads = self.threads
        logger.info("Concurrency: {} threads (target='{}')".format(
            num_threads, self.project.get_target().get('name'))
        )

        existing = adapter.query_for_existing(profile, schema_name)
        node_runners = self.get_runners(Runner, adapter, node_dependency_list)

        pool = ThreadPool(num_threads)
        node_results = []
        for node_list in node_dependency_list:
            runners = [node_runners[n.get('unique_id')] for n in node_list]

            args_list = [{'runner': runner, 'existing': existing, 'flat_graph': flat_graph} for runner in runners] # noqa

            try:
                for result in pool.imap_unordered(self.call_runner, args_list):
                    node_results.append(result)

                    # propagate so that CTEs get injected properly
                    # TODO : is how does this work now????? Did we pass flat_graph by reference?
                    # is that terrible? Will this just work??
                    node_id = result.node.get('unique_id')
                    flat_graph['nodes'][node_id] = result.node

                    if result.errored:
                        #on_failure(result.node)
                        print("FAIL: ", result.node) # TODO TODO TODO
                        logger.info(result.error)

            except KeyboardInterrupt:
                pool.close()
                pool.terminate()

                profile = self.project.run_environment()
                adapter = get_adapter(profile)

                for conn_name in adapter.cancel_open_connections(profile):
                    dbt.ui.printer.print_cancel_line(conn_name, schema_name)

                pool.join()
                raise

        pool.close()
        pool.join()

        return node_results

    def run_from_graph(self, Selector, Runner, query):
        compiler = dbt.compilation.Compiler(self.project)
        compiler.initialize()
        (flat_graph, linker) = compiler.compile()

        selector = Selector(linker, flat_graph)
        selected_nodes = selector.select(query)
        dependency_list = selector.as_node_list(selected_nodes)

        profile = self.project.run_environment()
        adapter = get_adapter(profile)

        flat_nodes = dbt.utils.flatten_nodes(dependency_list)
        if len(flat_nodes) == 0:
            logger.info("WARNING: Nothing to do. Try checking your model "
                        "configs and model specification args")
            return []
        else:
            stat_line = dbt.ui.printer.get_counts(flat_nodes)
            full_line = "{} {}".format(Runner.verb, stat_line)

            logger.info("")
            dbt.ui.printer.print_timestamped_line(full_line)
            dbt.ui.printer.print_timestamped_line("")

        try:
            Runner.before_run(self.project, adapter, flat_graph)
            started = time.time()
            results = self.execute_nodes(Runner, flat_graph, dependency_list)
            elapsed = time.time() - started
            Runner.after_run(self.project, adapter, results, flat_graph, elapsed)

        finally:
            adapter.cleanup_connections()

        return results

    # ------------------------------------

    def run(self, query, Runner):
        Selector = NodeSelector
        return self.run_from_graph(Selector, Runner, query)

    def run_flat(self, query, Runner):
        Selector = FlatNodeSelector
        return self.run_from_graph(Selector, Runner, query)
