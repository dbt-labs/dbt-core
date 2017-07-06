import os
import time

from dbt.adapters.factory import get_adapter
from dbt.logger import GLOBAL_LOGGER as logger

from dbt.utils import get_materialization

import dbt.clients.jinja
import dbt.compilation
import dbt.exceptions
import dbt.linker
import dbt.tracking
import dbt.model
import dbt.ui.printer

import dbt.graph.selector

from multiprocessing.dummy import Pool as ThreadPool


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

    def get_dependent(self, linker, node_id):
        dependent_nodes = linker.get_dependent_nodes(node_id)
        for node_id in dependent_nodes:
            yield node_id

    def get_runners(self, Runner, adapter, node_dependency_list):
        all_nodes = dbt.utils.flatten_nodes(node_dependency_list)

        num_nodes = len([
            n for n in all_nodes if get_materialization(n) != 'ephemeral'
        ])

        node_runners = {}
        i = 0
        for node in all_nodes:
            uid = node.get('unique_id')
            if get_materialization(node) == 'ephemeral':
                runner = Runner(self.project, adapter, node, 0, 0)
            else:
                i += 1
                runner = Runner(self.project, adapter, node, i, num_nodes)
            node_runners[uid] = runner

        return node_runners

    def call_runner(self, data):
        runner = data['runner']
        existing = data['existing']
        flat_graph = data['flat_graph']

        if runner.skip:
            return runner.on_skip()

        # no before/after printing for ephemeral mdoels
        if runner.is_ephemeral():
            result = runner.safe_run(flat_graph, existing)
        else:
            runner.before_execute()
            result = runner.safe_run(flat_graph, existing)
            runner.after_execute(result)

        if result.errored and runner.raise_on_first_error():
            raise dbt.exceptions.RuntimeException(result.error)
        elif result.errored:
            logger.info(result.error)

        return result

    def get_relevant_runners(self, node_runners, node_subset):
        runners = []
        for node in node_subset:
            unique_id = node.get('unique_id')
            if unique_id in node_runners:
                runners.append(node_runners[unique_id])
        return runners

    def execute_nodes(self, linker, Runner, flat_graph, node_dependency_list):
        profile = self.project.run_environment()
        adapter = get_adapter(profile)
        schema_name = adapter.get_default_schema(profile)

        num_threads = self.threads
        target_name = self.project.get_target().get('name')

        text = "Concurrency: {} threads (target='{}')"
        concurrency_line = text.format(num_threads, target_name)
        dbt.ui.printer.print_timestamped_line(concurrency_line)
        dbt.ui.printer.print_timestamped_line("")

        existing = adapter.query_for_existing(profile, schema_name)
        node_runners = self.get_runners(Runner, adapter, node_dependency_list)

        pool = ThreadPool(num_threads)
        node_results = []
        for node_list in node_dependency_list:
            runners = self.get_relevant_runners(node_runners, node_list)

            args_list = []
            for runner in runners:
                args_list.append({
                    'existing': existing,
                    'flat_graph': flat_graph,
                    'runner': runner
                })

            try:
                for result in pool.imap_unordered(self.call_runner, args_list):
                    node_results.append(result)

                    node_id = result.node.get('unique_id')
                    flat_graph['nodes'][node_id] = result.node

                    if result.errored:
                        for dep_node_id in self.get_dependent(linker, node_id):
                            runner = node_runners.get(dep_node_id)
                            if runner:
                                runner.do_skip()

            except KeyboardInterrupt:
                pool.close()
                pool.terminate()

                profile = self.project.run_environment()
                adapter = get_adapter(profile)

                if not adapter.is_cancelable():
                    msg = ("The {} adapter does not support query "
                           "cancellation. Some queries may still be "
                           "running!".format(adapter.type()))

                    yellow = dbt.ui.printer.COLOR_FG_YELLOW
                    dbt.ui.printer.print_timestamped_line(msg, yellow)
                    raise

                for conn_name in adapter.cancel_open_connections(profile):
                    dbt.ui.printer.print_cancel_line(conn_name, schema_name)

                pool.join()
                raise

        pool.close()
        pool.join()

        return node_results

    def compile(self, project):
        compiler = dbt.compilation.Compiler(project)
        compiler.initialize()
        (flat_graph, linker) = compiler.compile()
        return (flat_graph, linker)

    def run_from_graph(self, Selector, Runner, query):
        flat_graph, linker = self.compile(self.project)

        selector = Selector(linker, flat_graph)
        selected_nodes = selector.select(query)
        dep_list = selector.as_node_list(selected_nodes)

        profile = self.project.run_environment()
        adapter = get_adapter(profile)

        flat_nodes = dbt.utils.flatten_nodes(dep_list)
        if len(flat_nodes) == 0:
            logger.info("WARNING: Nothing to do. Try checking your model "
                        "configs and model specification args")
            return []
        elif Runner.print_header:
            stat_line = dbt.ui.printer.get_counts(flat_nodes)
            logger.info("")
            dbt.ui.printer.print_timestamped_line(stat_line)
            dbt.ui.printer.print_timestamped_line("")
        else:
            logger.info("")

        try:
            Runner.before_run(self.project, adapter, flat_graph)
            started = time.time()
            res = self.execute_nodes(linker, Runner, flat_graph, dep_list)
            elapsed = time.time() - started
            Runner.after_run(self.project, adapter, res, flat_graph, elapsed)

        finally:
            adapter.cleanup_connections()

        return res

    # ------------------------------------

    def run(self, query, Runner):
        Selector = dbt.graph.selector.NodeSelector
        return self.run_from_graph(Selector, Runner, query)

    def run_flat(self, query, Runner):
        Selector = dbt.graph.selector.FlatNodeSelector
        return self.run_from_graph(Selector, Runner, query)
