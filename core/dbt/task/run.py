from __future__ import print_function

from dbt.logger import GLOBAL_LOGGER as logger
from dbt.node_types import NodeType, RunHookType
from dbt.node_runners import ModelRunner

import dbt.exceptions
import dbt.flags
import dbt.ui.printer
from dbt.contracts.graph.parsed import Hook
from dbt.hooks import get_hook_dict

from dbt.compilation import compile_node
from dbt.task.compile import CompileTask
from dbt.utils import get_nodes_by_tags


class RunTask(CompileTask):
    def raise_on_first_error(self):
        return False

    def populate_adapter_cache(self, adapter):
        adapter.set_relations_cache(self.manifest)

    def run_hooks(self, adapter, hook_type, extra_context):

        nodes = self.manifest.nodes.values()
        hooks = get_nodes_by_tags(nodes, {hook_type}, NodeType.Operation)

        ordered_hooks = sorted(hooks, key=lambda h: h.get('index', len(hooks)))

        for i, hook in enumerate(ordered_hooks):
            model_name = hook.get('name')

            # This will clear out an open transaction if there is one.
            # on-run-* hooks should run outside of a transaction. This happens
            # b/c psycopg2 automatically begins a transaction when a connection
            # is created. TODO : Move transaction logic out of here, and
            # implement a for-loop over these sql statements in jinja-land.
            # Also, consider configuring psycopg2 (and other adapters?) to
            # ensure that a transaction is only created if dbt initiates it.
            adapter.clear_transaction(model_name)
            compiled = compile_node(adapter, self.config, hook, self.manifest,
                                    extra_context)
            statement = compiled.wrapped_sql

            hook_index = hook.get('index', len(hooks))
            hook_dict = get_hook_dict(statement, index=hook_index)

            if dbt.flags.STRICT_MODE:
                Hook(**hook_dict)

            sql = hook_dict.get('sql', '')

            if len(sql.strip()) > 0:
                adapter.execute(sql, model_name=model_name, auto_begin=False,
                                fetch=False)

            adapter.release_connection(model_name)

    def safe_run_hooks(self, adapter, hook_type, extra_context):
        try:
            self.run_hooks(adapter, hook_type, extra_context)
        except dbt.exceptions.RuntimeException:
            logger.info("Database error while running {}".format(hook_type))
            raise

    @classmethod
    def print_results_line(cls, results, execution_time):
        nodes = [r.node for r in results]
        stat_line = dbt.ui.printer.get_counts(nodes)

        execution = ""

        if execution_time is not None:
            execution = " in {execution_time:0.2f}s".format(
                execution_time=execution_time)

        dbt.ui.printer.print_timestamped_line("")
        dbt.ui.printer.print_timestamped_line(
            "Finished running {stat_line}{execution}."
            .format(stat_line=stat_line, execution=execution))

    def before_run(self, adapter, selected_uids):
        self.populate_adapter_cache(adapter)
        self.safe_run_hooks(adapter, RunHookType.Start, {})
        self.create_schemas(adapter, selected_uids)

    def after_run(self, adapter, results):
        # in on-run-end hooks, provide the value 'schemas', which is a list of
        # unique schemas that successfully executed models were in
        # errored failed skipped
        schemas = list(set(
            r.node.schema for r in results
            if not any((r.error is not None, r.failed, r.skipped))
        ))
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
            dbt.ui.printer.print_run_end_messages(results)
