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
import dbt.ui.printer
import dbt.utils
from dbt.clients.system import write_json

import dbt.graph.selector

from multiprocessing.dummy import Pool as ThreadPool


RESULT_FILE_NAME = 'run_results.json'


class RunBuilder(object):
    """Given a node, build a Runner, tracking the number built so far vs the
    total.

    # TODO: this name completely sucks
    """
    def __init__(self, flattened_nodes, Runner, config):
        self.run_count = 0
        num_nodes = len([
            n for n in flattened_nodes
            if not Runner.is_ephemeral_model(n)
        ])
        self.num_nodes = num_nodes
        self.Runner = Runner
        self.config = config
        self.adapter = get_adapter(config)
        # TODO: do we need to track this?
        self._created = {}

    def get_runner(self, node):
        if self.Runner.is_ephemeral_model(node):
            runner = self.Runner(self.config, self.adapter, node,
                                 0, 0)
        else:
            self.run_count += 1
            runner = self.Runner(self.config, self.adapter, node,
                                 self.run_count, self.num_nodes)
        self._created[node.unique_id] = runner
        return runner


class RunManager(object):
    def __init__(self, config, query, Runner, flat=False):
        """
        Runner is a type (not instance!) derived from
            dbt.node_runners.BaseRunner

        if flat is set, nodes will be selected using the FlatNodeSelector
        """
        self.config = config
        self.query = query
        self.Runner = Runner
        self.run_count = 0

        if flat:
            Selector = dbt.graph.selector.FlatNodeSelector
        else:
            Selector = dbt.graph.selector.NodeSelector

        manifest, linker = self.compile(self.config)
        self.manifest = manifest
        self.linker = linker

        selector = Selector(linker, manifest)
        selected_nodes = selector.select(query)
        # TODO: swap this out for run_graph stuff once I verify this refactor
        self.dependency_list = selector.as_node_list(selected_nodes)
        # we use this a couple times.
        self._flattened_nodes = dbt.utils.flatten_nodes(self.dependency_list)
        self._builder = RunBuilder(self._flattened_nodes, Runner, config)

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

    def get_relevant_runners(self, node_subset):
        runners = []
        for node in node_subset:
            runners.append(self._builder.get_runner(node))
        return runners

    def map_run(self, pool, args):
        """If the caller has passed the magic 'single-threaded' flag, use map()
        instead of the pool.imap_unordered. The single-threaded flag is
        intended for gathering more useful performance information about what
        happens beneath `call_runner`, since python's default profiling tools
        ignore child threads.
        """
        if self.config.args.single_threaded:
            return map(self.call_runner, args)
        else:
            return pool.imap_unordered(self.call_runner, args)

    def execute_nodes(self):
        adapter = get_adapter(self.config)

        num_threads = self.config.threads
        target_name = self.config.target_name

        text = "Concurrency: {} threads (target='{}')"
        concurrency_line = text.format(num_threads, target_name)
        dbt.ui.printer.print_timestamped_line(concurrency_line)
        dbt.ui.printer.print_timestamped_line("")

        schemas = list(self.Runner.get_model_schemas(self.manifest))

        pool = ThreadPool(num_threads)
        node_results = []
        for node_list in self.dependency_list:
            runners = self.get_relevant_runners(node_list)

            args_list = []
            for runner in runners:
                args_list.append({
                    'manifest': self.manifest,
                    'runner': runner
                })

            try:
                for result in self.map_run(pool, args_list):
                    is_ephemeral = self.Runner.is_ephemeral_model(result.node)
                    if not is_ephemeral:
                        node_results.append(result)

                    node = CompileResultNode(**result.node)
                    node_id = node.unique_id
                    self.manifest.nodes[node_id] = node

                    if result.errored:
                        dependents = self.get_dependent(self.linker, node_id)
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

    def run(self):
        """
        Run dbt for the query, based on the graph.
        """
        adapter = get_adapter(self.config)

        if len(self._flattened_nodes) == 0:
            logger.info("WARNING: Nothing to do. Try checking your model "
                        "configs and model specification args")
            return []
        elif self.Runner.print_header:
            stat_line = dbt.ui.printer.get_counts(self._flattened_nodes)
            logger.info("")
            dbt.ui.printer.print_timestamped_line(stat_line)
            dbt.ui.printer.print_timestamped_line("")
        else:
            logger.info("")

        try:
            self.Runner.before_hooks(self.config, adapter, self.manifest)
            started = time.time()
            self.Runner.before_run(self.config, adapter, self.manifest)
            res = self.execute_nodes()
            self.Runner.after_run(self.config, adapter, res, self.manifest)
            elapsed = time.time() - started
            self.Runner.after_hooks(self.config, adapter, res, self.manifest,
                                    elapsed)

        finally:
            adapter.cleanup_connections()

        result = ExecutionResult(
            results=res,
            elapsed_time=elapsed,
            generated_at=dbt.utils.timestring(),
        )
        self.write_results(result)

        return res
