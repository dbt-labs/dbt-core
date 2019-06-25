import base64
import os
import time
from abc import abstractmethod
from multiprocessing.dummy import Pool as ThreadPool

from dbt import rpc
from dbt.task.base import ConfiguredTask
from dbt.adapters.factory import get_adapter
from dbt.logger import GLOBAL_LOGGER as logger
from dbt.compilation import compile_manifest
from dbt.contracts.graph.manifest import CompileResultNode
from dbt.contracts.results import ExecutionResult
from dbt.loader import GraphLoader

import dbt.exceptions
import dbt.ui.printer
import dbt.utils

import dbt.graph.selector

RESULT_FILE_NAME = 'run_results.json'
MANIFEST_FILE_NAME = 'manifest.json'


def load_manifest(config):
    # performance trick: if the adapter has a manifest loaded, use that to
    # avoid parsing internal macros twice.
    internal_manifest = get_adapter(config).check_internal_manifest()
    manifest = GraphLoader.load_all(config,
                                    internal_manifest=internal_manifest)

    manifest.write(os.path.join(config.target_path, MANIFEST_FILE_NAME))
    return manifest


class ManifestTask(ConfiguredTask):
    def __init__(self, args, config):
        super().__init__(args, config)
        self.manifest = None
        self.linker = None

    def _runtime_initialize(self):
        self.manifest = load_manifest(self.config)
        self.linker = compile_manifest(self.config, self.manifest)


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
        # TODO: create+enforce an actual contracts for what `result` is instead
        # of the current free-for-all
        result = runner.run_with_hooks(self.manifest)
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

        node = CompileResultNode(**result.node)
        node_id = node.unique_id
        self.manifest.nodes[node_id] = node

        if result.error is not None:
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
            generated_at=dbt.utils.timestring()
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
            return []
        else:
            logger.info("")

        selected_uids = frozenset(n.unique_id for n in self._flattened_nodes)
        result = self.execute_with_hooks(selected_uids)

        result.write(self.result_path())

        self.task_end_messages(result.results)
        return result.results

    def interpret_results(self, results):
        if results is None:
            return False

        failures = [r for r in results if r.error or r.failed]
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

        # Snowflake needs to issue a "use {schema}" query, where schema
        # is the one defined in the profile. Create this schema if it
        # does not exist, otherwise subsequent queries will fail. Generally,
        # dbt expects that this schema will exist anyway.
        required_schemas.add(
            (self.config.credentials.database, self.config.credentials.schema)
        )

        required_databases = set(db for db, _ in required_schemas)

        existing_schemas = set()
        for db in required_databases:
            existing_schemas.update((db, s) for s in adapter.list_schemas(db))

        for database, schema in (required_schemas - existing_schemas):
            adapter.create_schema(database, schema)

    def get_result(self, results, elapsed_time, generated_at):
        return ExecutionResult(
            results=results,
            elapsed_time=elapsed_time,
            generated_at=generated_at
        )

    def task_end_messages(self, results):
        dbt.ui.printer.print_run_end_messages(results)


class RemoteCallable:
    METHOD_NAME = None
    is_async = False

    @abstractmethod
    def handle_request(self, **kwargs):
        raise dbt.exceptions.NotImplementedException(
            'from_kwargs not implemented'
        )

    def decode_sql(self, sql):
        """Base64 decode a string. This should only be used for sql in calls.

        :param str sql: The base64 encoded form of the original utf-8 string
        :return str: The decoded utf-8 string
        """
        # JSON is defined as using "unicode", we'll go a step further and
        # mandate utf-8 (though for the base64 part, it doesn't really matter!)
        base64_sql_bytes = str(sql).encode('utf-8')

        try:
            sql_bytes = base64.b64decode(base64_sql_bytes, validate=True)
        except ValueError:
            self.raise_invalid_base64(sql)

        return sql_bytes.decode('utf-8')

    @staticmethod
    def raise_invalid_base64(sql):
        raise rpc.invalid_params(
            data={
                'message': 'invalid base64-encoded sql input',
                'sql': str(sql),
            }
        )
