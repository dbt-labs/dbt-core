import os
import time

from dbt.adapters.factory import get_adapter
from dbt.logger import GLOBAL_LOGGER as logger
from dbt.contracts.graph.parsed import ParsedNode
from dbt.contracts.graph.manifest import CompileResultNode
from dbt.contracts.results import ExecutionResult

import dbt.clients.jinja
import dbt.compilation
import dbt.exceptions
import dbt.linker
import dbt.tracking
import dbt.model
import dbt.ui.printer
import dbt.utils
from dbt.clients.system import write_json

import dbt.graph.selector

from multiprocessing.dummy import Pool as ThreadPool


RESULT_FILE_NAME = 'run_results.json'


class RunManager(object):
    def __init__(self, config):
        self.config = config

    def deserialize_graph(self):
        logger.info("Loading dependency graph file.")

        base_target_path = self.config.target_path
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
            n for n in all_nodes if not Runner.is_ephemeral_model(n)
        ])

        node_runners = {}
        i = 0
        for node in all_nodes:
            uid = node.get('unique_id')
            if Runner.is_ephemeral_model(node):
                runner = Runner(self.config, adapter, node, 0, 0)
            else:
                i += 1
                runner = Runner(self.config, adapter, node, i, num_nodes)
            node_runners[uid] = runner

        return node_runners

    def call_runner(self, data):
        runner = data['runner']
        manifest = data['manifest']

        if runner.skip:
            return runner.on_skip()

        # no before/after printing for ephemeral mdoels
        if not runner.is_ephemeral_model(runner.node):
            runner.before_execute()

        result = runner.safe_run(manifest)

        if not runner.is_ephemeral_model(runner.node):
            runner.after_execute(result)

        if result.errored and runner.raise_on_first_error():
            raise dbt.exceptions.RuntimeException(result.error)

        return result

    def get_relevant_runners(self, node_runners, node_subset):
        runners = []
        for node in node_subset:
            unique_id = node.get('unique_id')
            if unique_id in node_runners:
                runners.append(node_runners[unique_id])
        return runners

    def execute_nodes(self, linker, Runner, manifest, node_dependency_list):
        adapter = get_adapter(self.config)

        num_threads = self.config.threads
        target_name = self.config.target_name

        text = "Concurrency: {} threads (target='{}')"
        concurrency_line = text.format(num_threads, target_name)
        dbt.ui.printer.print_timestamped_line(concurrency_line)
        dbt.ui.printer.print_timestamped_line("")

        schemas = list(Runner.get_model_schemas(manifest))
        node_runners = self.get_runners(Runner, adapter, node_dependency_list)

        pool = ThreadPool(num_threads)
        node_results = []
        for node_list in node_dependency_list:
            runners = self.get_relevant_runners(node_runners, node_list)

            args_list = []
            for runner in runners:
                args_list.append({
                    'manifest': manifest,
                    'runner': runner
                })

            try:
                for result in pool.imap_unordered(self.call_runner, args_list):
                    is_ephemeral = Runner.is_ephemeral_model(result.node)
                    if not is_ephemeral:
                        node_results.append(result)

                    node = CompileResultNode(**result.node)
                    node_id = node.unique_id
                    manifest.nodes[node_id] = node

                    if result.errored:
                        dependents = self.get_dependent(linker, node_id)
                        self._mark_dependent_errors(node_runners, dependents,
                                                    result, is_ephemeral)

            except KeyboardInterrupt:
                pool.close()
                pool.terminate()

                adapter = get_adapter(self.config)

                if not adapter.is_cancelable():
                    msg = ("The {} adapter does not support query "
                           "cancellation. Some queries may still be "
                           "running!".format(adapter.type()))

                    yellow = dbt.ui.printer.COLOR_FG_YELLOW
                    dbt.ui.printer.print_timestamped_line(msg, yellow)
                    raise

                for conn_name in adapter.cancel_open_connections():
                    dbt.ui.printer.print_cancel_line(conn_name)

                dbt.ui.printer.print_run_end_messages(node_results,
                                                      early_exit=True)

                pool.join()
                raise

        pool.close()
        pool.join()

        return node_results

    @staticmethod
    def _mark_dependent_errors(node_runners, dependents, result, is_ephemeral):
        for dep_node_id in dependents:
            runner = node_runners.get(dep_node_id)
            if not runner:
                continue
            if is_ephemeral:
                cause = result
            else:
                cause = None
            runner.do_skip(cause=result)

    def write_results(self, execution_result):
        filepath = os.path.join(self.config.target_path, RESULT_FILE_NAME)
        write_json(filepath, execution_result.serialize())

    def compile(self, config):
        compiler = dbt.compilation.Compiler(config)
        compiler.initialize()
        return compiler.compile()

    def run_from_graph(self, Selector, Runner, query):
        """
        Run dbt for the query, based on the graph.
        Selector is a type (not instance!) derived from
            dbt.graph.selector.NodeSelector
        Runner is a type (not instance!) derived from
            dbt.node_runners.BaseRunner

        """
        manifest, linker = self.compile(self.config)

        selector = Selector(linker, manifest)
        selected_nodes = selector.select(query)
        dep_list = selector.as_node_list(selected_nodes)

        adapter = get_adapter(self.config)

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
            Runner.before_hooks(self.config, adapter, manifest)
            started = time.time()
            Runner.before_run(self.config, adapter, manifest)
            res = self.execute_nodes(linker, Runner, manifest, dep_list)
            Runner.after_run(self.config, adapter, res, manifest)
            elapsed = time.time() - started
            Runner.after_hooks(self.config, adapter, res, manifest, elapsed)

        finally:
            adapter.cleanup_connections()

        result = ExecutionResult(
            results=res,
            elapsed_time=elapsed,
            generated_at=dbt.utils.timestring(),
        )
        self.write_results(result)

        return res

    # ------------------------------------

    def run(self, query, Runner):
        Selector = dbt.graph.selector.NodeSelector
        return self.run_from_graph(Selector, Runner, query)

    def run_flat(self, query, Runner):
        Selector = dbt.graph.selector.FlatNodeSelector
        return self.run_from_graph(Selector, Runner, query)
