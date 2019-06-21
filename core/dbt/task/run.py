from __future__ import print_function

import functools
import time

from dbt.logger import GLOBAL_LOGGER as logger
from dbt.node_types import NodeType, RunHookType
from dbt.node_runners import ModelRunner, RPCExecuteRunner

import dbt.exceptions
import dbt.flags
from dbt.contracts.graph.parsed import Hook
from dbt.hooks import get_hook_dict
from dbt.ui.printer import \
    print_hook_start_line, \
    print_hook_end_line, \
    print_timestamped_line, \
    print_run_end_messages, \
    get_counts

from dbt.compilation import compile_node
from dbt.task.compile import CompileTask, RemoteCompileTask
from dbt.utils import get_nodes_by_tags


class Timer:
    def __init__(self):
        self.start = None
        self.end = None

    @property
    def elapsed(self):
        if self.start is None or self.end is None:
            return None
        return self.end - self.start

    def __enter__(self):
        self.start = time.time()
        return self

    def __exit__(self, exc_type, exc_value, exc_tracebck):
        self.end = time.time()


@functools.total_ordering
class BiggestName:
    def __lt__(self, other):
        return True

    def __eq__(self, other):
        return isinstance(other, self.__class__)


class RunTask(CompileTask):
    def __init__(self, args, config):
        super().__init__(args, config)
        self.ran_hooks = []

    def raise_on_first_error(self):
        return False

    def populate_adapter_cache(self, adapter):
        adapter.set_relations_cache(self.manifest)

    def get_hook_sql(self, adapter, hook, idx, num_hooks, extra_context):
        compiled = compile_node(adapter, self.config, hook, self.manifest,
                                extra_context)
        statement = compiled.wrapped_sql
        hook_index = hook.get('index', num_hooks)
        hook_dict = get_hook_dict(statement, index=hook_index)
        if dbt.flags.STRICT_MODE:
            Hook(**hook_dict)
        return hook_dict.get('sql', '')

    def _hook_keyfunc(self, hook):
        package_name = hook.package_name
        if package_name == self.config.project_name:
            package_name = BiggestName()
        return package_name, hook.index

    def get_hooks_by_type(self, hook_type):
        nodes = self.manifest.nodes.values()
        # find all hooks defined in the manifest (could be multiple projects)
        hooks = get_nodes_by_tags(nodes, {hook_type}, NodeType.Operation)
        hooks.sort(key=self._hook_keyfunc)
        return hooks

    def run_hooks(self, adapter, hook_type, extra_context):
        ordered_hooks = self.get_hooks_by_type(hook_type)

        # on-run-* hooks should run outside of a transaction. This happens
        # b/c psycopg2 automatically begins a transaction when a connection
        # is created.
        adapter.clear_transaction()
        if not ordered_hooks:
            return
        num_hooks = len(ordered_hooks)

        plural = 'hook' if num_hooks == 1 else 'hooks'
        print_timestamped_line("")
        print_timestamped_line(
            'Running {} {} {}'.format(num_hooks, hook_type, plural)
        )

        for idx, hook in enumerate(ordered_hooks, start=1):
            sql = self.get_hook_sql(adapter, hook, idx, num_hooks,
                                    extra_context)

            hook_text = '{}.{}.{}'.format(hook.package_name, hook_type,
                                          hook.index)
            print_hook_start_line(hook_text, idx, num_hooks)
            status = 'OK'

            with Timer() as timer:
                if len(sql.strip()) > 0:
                    status, _ = adapter.execute(sql, auto_begin=False,
                                                fetch=False)
            self.ran_hooks.append(hook)

            print_hook_end_line(hook_text, status, idx, num_hooks,
                                timer.elapsed)

        print_timestamped_line("")

    def safe_run_hooks(self, adapter, hook_type, extra_context):
        try:
            self.run_hooks(adapter, hook_type, extra_context)
        except dbt.exceptions.RuntimeException:
            logger.info("Database error while running {}".format(hook_type))
            raise

    def print_results_line(self, results, execution_time):
        nodes = [r.node for r in results] + self.ran_hooks
        stat_line = get_counts(nodes)

        execution = ""

        if execution_time is not None:
            execution = " in {execution_time:0.2f}s".format(
                execution_time=execution_time)

        print_timestamped_line("")
        print_timestamped_line(
            "Finished running {stat_line}{execution}."
            .format(stat_line=stat_line, execution=execution))

    def before_run(self, adapter, selected_uids):
        with adapter.connection_named('master'):
            self.create_schemas(adapter, selected_uids)
            self.populate_adapter_cache(adapter)
            self.safe_run_hooks(adapter, RunHookType.Start, {})

    def after_run(self, adapter, results):
        # in on-run-end hooks, provide the value 'schemas', which is a list of
        # unique schemas that successfully executed models were in
        # errored failed skipped
        schemas = list(set(
            r.node.schema for r in results
            if not any((r.error is not None, r.failed, r.skipped))
        ))
        with adapter.connection_named('master'):
            self.safe_run_hooks(adapter, RunHookType.End,
                                {'schemas': schemas, 'results': results})

    def after_hooks(self, adapter, results, elapsed):
        self.print_results_line(results, elapsed)

    def build_query(self):
        return {
            "include": self.args.models,
            "exclude": self.args.exclude,
            "resource_types": [NodeType.Model],
            "tags": []
        }

    def get_runner_type(self):
        return ModelRunner

    def task_end_messages(self, results):
        if results:
            print_run_end_messages(results)


class RemoteRunTask(RemoteCompileTask, RunTask):
    METHOD_NAME = 'run'

    def get_runner_type(self):
        return RPCExecuteRunner
