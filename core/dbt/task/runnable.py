import os
import time
from datetime import datetime
from multiprocessing.dummy import Pool as ThreadPool

from dbt.task.base import ConfiguredTask
from dbt.adapters.factory import get_adapter
from dbt.logger import (
    GLOBAL_LOGGER as logger,
    DbtProcessState,
    TextOnly,
    UniqueID,
    TimestampNamed,
    DbtModelState,
    ModelMetadata,
    NodeCount,
)
from dbt.compilation import compile_manifest
from dbt.contracts.results import ExecutionResult
from dbt.perf_utils import get_full_manifest

import dbt.exceptions
import dbt.flags
import dbt.ui.printer
import dbt.utils

import dbt.graph.selector

RESULT_FILE_NAME = 'run_results.json'
MANIFEST_FILE_NAME = 'manifest.json'
RUNNING_STATE = DbtProcessState('running')


def write_manifest(config, manifest):
    if dbt.flags.WRITE_JSON:
        manifest.write(os.path.join(config.target_path, MANIFEST_FILE_NAME))


class ManifestTask(ConfiguredTask):
    def __init__(self, args, config):
        super().__init__(args, config)
        self.manifest = None
        self.linker = None

    def load_manifest(self):
        self.manifest = get_full_manifest(self.config)
        write_manifest(self.config, self.manifest)

    def compile_manifest(self):
        self.linker = compile_manifest(self.config, self.manifest)
        self.manifest.build_flat_graph()

    def _runtime_initialize(self):
        self.load_manifest()
        self.compile_manifest()


class GraphRunnableTask(ManifestTask):
    def __init__(self, args, config):
        super().__init__(args, config)
        self.job_queue = None
        self._flattened_nodes = None

        self.run_count = 0
        self.num_nodes = None
        self.node_results = []
        self._skipped_children = {}
        self._raise_next_tick = None

    def index_offset(self, value: int) -> int:
        return value

    def select_nodes(self):
        selector = dbt.graph.selector.NodeSelector(
            self.linker.graph, self.manifest
        )
        selected_nodes = selector.select(self.build_query())
        return selected_nodes

    def _runtime_initialize(self):
        super()._runtime_initialize()
        selected_nodes = self.select_nodes()
        self.job_queue = self.linker.as_graph_queue(self.manifest,
                                                    selected_nodes)

        # we use this a couple times. order does not matter.
        self._flattened_nodes = [
            self.manifest.nodes[uid] for uid in selected_nodes
        ]

        self.num_nodes = len([
            n for n in self._flattened_nodes
            if not n.is_ephemeral_model
        ])

    def raise_on_first_error(self):
        return False

    def build_query(self):
        raise dbt.exceptions.NotImplementedException('Not Implemented')

    def get_runner_type(self):
        raise dbt.exceptions.NotImplementedException('Not Implemented')

    def result_path(self):
        return os.path.join(self.config.target_path, RESULT_FILE_NAME)

    def get_runner(self, node):
        adapter = get_adapter(self.config)

        if node.is_ephemeral_model:
            run_count = 0
            num_nodes = 0
        else:
            self.run_count += 1
            run_count = self.run_count
            num_nodes = self.num_nodes

        cls = self.get_runner_type()
        return cls(self.config, adapter, node, run_count, num_nodes)

    def call_runner(self, runner):
        uid_context = UniqueID(runner.node.unique_id)
        with RUNNING_STATE, uid_context:
            startctx = TimestampNamed('node_started_at')
            index = self.index_offset(runner.node_index)
            extended_metadata = ModelMetadata(runner.node, index)
            with startctx, extended_metadata:
                logger.debug('Began running node {}'.format(
                    runner.node.unique_id))
            status = 'error'  # we must have an error if we don't see this
            try:
                result = runner.run_with_hooks(self.manifest)
                status = runner.get_result_status(result)
            finally:
                finishctx = TimestampNamed('node_finished_at')
                with finishctx, DbtModelState(status):
                    logger.debug('Finished running node {}'.format(
                        runner.node.unique_id))
        if result.error is not None and self.raise_on_first_error():
            # if we raise inside a thread, it'll just get silently swallowed.
            # stash the error message we want here, and it will check the
            # next 'tick' - should be soon since our thread is about to finish!
            self._raise_next_tick = result.error

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

    def _raise_set_error(self):
        if self._raise_next_tick is not None:
            raise dbt.exceptions.RuntimeException(self._raise_next_tick)

    def run_queue(self, pool):
        """Given a pool, submit jobs from the queue to the pool.
        """
        def callback(result):
            """Note: mark_done, at a minimum, must happen here or dbt will
            deadlock during ephemeral result error handling!
            """
            self._handle_result(result)
            self.job_queue.mark_done(result.node.unique_id)

        while not self.job_queue.empty():
            node = self.job_queue.get()
            self._raise_set_error()
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
        # if an error got set during join(), raise it.
        self._raise_set_error()

        return

    def _handle_result(self, result):
        """Mark the result as completed, insert the `CompiledResultNode` into
        the manifest, and mark any descendants (potentially with a 'cause' if
        the result was an ephemeral model) as skipped.
        """
        is_ephemeral = result.node.is_ephemeral_model
        if not is_ephemeral:
            self.node_results.append(result)

        node = result.node
        self.manifest.update_node(node)

        if result.error is not None:
            if is_ephemeral:
                cause = result
            else:
                cause = None
            self._mark_dependent_errors(node.unique_id, result, cause)

    def execute_nodes(self):
        num_threads = self.config.threads
        target_name = self.config.target_name

        text = "Concurrency: {} threads (target='{}')"
        concurrency_line = text.format(num_threads, target_name)
        with NodeCount(self.num_nodes):
            dbt.ui.printer.print_timestamped_line(concurrency_line)
        with TextOnly():
            dbt.ui.printer.print_timestamped_line("")

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
                if self.manifest is not None:
                    node = self.manifest.nodes.get(conn_name)
                    if node is not None and node.is_ephemeral_model:
                        continue
                # if we don't have a manifest/don't have a node, print anyway.
                dbt.ui.printer.print_cancel_line(conn_name)

            pool.join()

            dbt.ui.printer.print_run_end_messages(self.node_results,
                                                  early_exit=True)

            raise

        pool.close()
        pool.join()

        return self.node_results

    def _mark_dependent_errors(self, node_id, result, cause):
        for dep_node_id in self.linker.get_dependent_nodes(node_id):
            self._skipped_children[dep_node_id] = cause

    def before_hooks(self, adapter):
        pass

    def before_run(self, adapter, selected_uids):
        pass

    def after_run(self, adapter, results):
        pass

    def after_hooks(self, adapter, results, elapsed):
        pass

    def execute_with_hooks(self, selected_uids):
        adapter = get_adapter(self.config)
        try:
            self.before_hooks(adapter)
            started = time.time()
            self.before_run(adapter, selected_uids)
            res = self.execute_nodes()
            self.after_run(adapter, res)
            elapsed = time.time() - started
            self.after_hooks(adapter, res, elapsed)

        finally:
            adapter.cleanup_connections()

        result = self.get_result(
            results=res,
            elapsed_time=elapsed,
            generated_at=datetime.utcnow()
        )
        return result

    def run(self):
        """
        Run dbt for the query, based on the graph.
        """
        self._runtime_initialize()

        if len(self._flattened_nodes) == 0:
            logger.warning("WARNING: Nothing to do. Try checking your model "
                           "configs and model specification args")
            return self.get_result(
                results=[],
                generated_at=datetime.utcnow(),
                elapsed_time=0.0,
            )
        else:
            with TextOnly():
                logger.info("")

        selected_uids = frozenset(n.unique_id for n in self._flattened_nodes)
        result = self.execute_with_hooks(selected_uids)

        if dbt.flags.WRITE_JSON:
            result.write(self.result_path())

        self.task_end_messages(result.results)
        return result

    def interpret_results(self, results):
        if results is None:
            return False

        failures = [r for r in results if r.error or r.fail]
        return len(failures) == 0

    def get_model_schemas(self, selected_uids):
        schemas = set()
        for node in self.manifest.nodes.values():
            if node.unique_id not in selected_uids:
                continue
            if node.is_refable and not node.is_ephemeral:
                schemas.add((node.database, node.schema))

        return schemas

    def create_schemas(self, adapter, selected_uids):
        required_schemas = self.get_model_schemas(selected_uids)
        required_databases = set(db for db, _ in required_schemas)

        existing_schemas_lowered = set()
        for db in required_databases:
            existing_schemas_lowered.update(
                (db.lower(), s.lower()) for s in adapter.list_schemas(db))

        for db, schema in required_schemas:
            if (db.lower(), schema.lower()) not in existing_schemas_lowered:
                adapter.create_schema(db, schema)

    def get_result(self, results, elapsed_time, generated_at):
        return ExecutionResult(
            results=results,
            elapsed_time=elapsed_time,
            generated_at=generated_at
        )

    def task_end_messages(self, results):
        dbt.ui.printer.print_run_end_messages(results)
