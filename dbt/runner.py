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


class RunManager(object):
    def __init__(self, config, query, Runner):
        """
        Runner is a type (not instance!) derived from
            dbt.node_runners.BaseRunner
        """
        self.config = config
        self.query = query
        self.Runner = Runner

        manifest, linker = self.compile(self.config)
        self.manifest = manifest
        self.linker = linker

        selector = dbt.graph.selector.NodeSelector(linker, manifest)
        selected_nodes = selector.select(query)
        self.job_queue = self.linker.as_graph_queue(manifest, selected_nodes)

        # we use this a couple times. order does not matter.
        self._flattened_nodes = [
            self.manifest.nodes[uid] for uid in selected_nodes
        ]

        self.run_count = 0
        self.num_nodes = len([
            n for n in self._flattened_nodes
            if not Runner.is_ephemeral_model(n)
        ])
        self.node_results = []
        self._skipped_children = {}

    def get_runner(self, node):
        adapter = get_adapter(self.config)

        if self.Runner.is_ephemeral_model(node):
            run_count = 0
            num_nodes = 0
        else:
            self.run_count += 1
            run_count = self.run_count
            num_nodes = self.num_nodes

        return self.Runner(self.config, adapter, node, run_count, num_nodes)

    def call_runner(self, runner):
        if runner.skip:
            return runner.on_skip()

        # no before/after printing for ephemeral mdoels
        if not runner.is_ephemeral_model(runner.node):
            runner.before_execute()

        result = runner.safe_run(self.manifest)

        if not runner.is_ephemeral_model(runner.node):
            runner.after_execute(result)

        if result.errored and runner.raise_on_first_error():
            raise dbt.exceptions.RuntimeException(result.error)

        return result

    def _submit(self, pool, args, callback):
        """If the caller has passed the magic 'single-threaded' flag, call the
        function directly instead of pool.apply_async. The single-threaded flag
         is intended for gathering more useful performance information about
        what appens beneath `call_runner`, since python's default profiling
        tools ignore child threads.

        This does still go through the callback path for result collection.
        """
        if self.config.args.single_threaded:
            callback(self.call_runner(*args))
        else:
            pool.apply_async(self.call_runner, args=args, callback=callback)

    def run_queue(self, pool):
        """Given a pool, submit jobs from the queue to the pool.
        """
        def callback(result):
            """A callback to handle results."""
            self._handle_result(result)
            self.job_queue.mark_done(result.node.unique_id)

        while not self.job_queue.empty():
            node = self.job_queue.get()
            runner = self.get_runner(node)
            # we finally know what we're running! Make sure we haven't decided
            # to skip it due to upstream failures
            if runner.node.unique_id in self._skipped_children:
                cause = self._skipped_children.pop(runner.node.unique_id)
                runner.do_skip(cause=cause)
            args = (runner,)
            self._submit(pool, args, callback)
        # block on completion
        self.job_queue.join()

        return

    def _handle_result(self, result):
        """Note: this happens inside an apply_async() callback, so it must be
        "fast". (The pool worker thread will block!)
        """
        is_ephemeral = self.Runner.is_ephemeral_model(result.node)
        if not is_ephemeral:
            self.node_results.append(result)

        node = CompileResultNode(**result.node)
        node_id = node.unique_id
        self.manifest.nodes[node_id] = node

        if result.errored:
            if is_ephemeral:
                cause = result
            else:
                cause = None
            self._mark_dependent_errors(node_id, result, cause)

    def execute_nodes(self):
        num_threads = self.config.threads
        target_name = self.config.target_name

        text = "Concurrency: {} threads (target='{}')"
        concurrency_line = text.format(num_threads, target_name)
        dbt.ui.printer.print_timestamped_line(concurrency_line)
        dbt.ui.printer.print_timestamped_line("")

        schemas = list(self.Runner.get_model_schemas(self.manifest))

        pool = ThreadPool(num_threads)
        try:
            self.run_queue(pool)

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

            dbt.ui.printer.print_run_end_messages(self.node_results,
                                                  early_exit=True)

            pool.join()
            raise

        pool.close()
        pool.join()

        return self.node_results

    def _mark_dependent_errors(self, node_id, result, cause):
        for dep_node_id in self.linker.get_dependent_nodes(node_id):
            self._skipped_children[dep_node_id] = cause

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
