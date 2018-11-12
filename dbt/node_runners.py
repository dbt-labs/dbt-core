
from dbt.logger import GLOBAL_LOGGER as logger
from dbt.exceptions import NotImplementedException
from dbt.utils import get_nodes_by_tags
from dbt.node_types import NodeType, RunHookType
from dbt.adapters.factory import get_adapter
from dbt.contracts.results import RunModelResult

import dbt.clients.jinja
import dbt.context.runtime
import dbt.utils
import dbt.tracking
import dbt.ui.printer
import dbt.flags
import dbt.schema
import dbt.templates
import dbt.writer

import six
import sys
import time
import traceback


INTERNAL_ERROR_STRING = """This is an error in dbt. Please try again. If \
the error persists, open an issue at https://github.com/fishtown-analytics/dbt
""".strip()


def track_model_run(index, num_nodes, run_model_result):
    invocation_id = dbt.tracking.active_user.invocation_id
    dbt.tracking.track_model_run({
        "invocation_id": invocation_id,
        "index": index,
        "total": num_nodes,
        "execution_time": run_model_result.execution_time,
        "run_status": run_model_result.status,
        "run_skipped": run_model_result.skip,
        "run_error": None,
        "model_materialization": dbt.utils.get_materialization(run_model_result.node),  # noqa
        "model_id": dbt.utils.get_hash(run_model_result.node),
        "hashed_contents": dbt.utils.get_hashed_contents(run_model_result.node),  # noqa
    })


class BaseRunner(object):
    print_header = True

    def __init__(self, config, adapter, node, node_index, num_nodes):
        self.config = config
        self.adapter = adapter
        self.node = node
        self.node_index = node_index
        self.num_nodes = num_nodes

        self.skip = False
        self.skip_cause = None

    def raise_on_first_error(self):
        return False

    @classmethod
    def is_refable(cls, node):
        return node.resource_type in NodeType.refable()

    @classmethod
    def is_ephemeral(cls, node):
        return dbt.utils.get_materialization(node) == 'ephemeral'

    @classmethod
    def is_ephemeral_model(cls, node):
        return cls.is_refable(node) and cls.is_ephemeral(node)

    def safe_run(self, manifest):
        catchable_errors = (dbt.exceptions.CompilationException,
                            dbt.exceptions.RuntimeException)

        result = RunModelResult(self.node)
        started = time.time()
        exc_info = (None, None, None)

        try:
            # if we fail here, we still have a compiled node to return
            # this has the benefit of showing a build path for the errant model
            compiled_node = self.compile(manifest)
            result.node = compiled_node

            # for ephemeral nodes, we only want to compile, not run
            if not self.is_ephemeral_model(self.node):
                result = self.run(compiled_node, manifest)

        except catchable_errors as e:
            if e.node is None:
                e.node = result.node

            result.error = dbt.compat.to_string(e)
            result.status = 'ERROR'

        except dbt.exceptions.InternalException as e:
            build_path = self.node.build_path
            prefix = 'Internal error executing {}'.format(build_path)

            error = "{prefix}\n{error}\n\n{note}".format(
                         prefix=dbt.ui.printer.red(prefix),
                         error=str(e).strip(),
                         note=INTERNAL_ERROR_STRING)
            logger.debug(error)

            result.error = dbt.compat.to_string(e)
            result.status = 'ERROR'

        except Exception as e:
            # set this here instead of finally, as python 2/3 exc_info()
            # behavior with re-raised exceptions are slightly different
            exc_info = sys.exc_info()
            prefix = "Unhandled error while executing {filepath}".format(
                        filepath=self.node.build_path)

            error = "{prefix}\n{error}".format(
                         prefix=dbt.ui.printer.red(prefix),
                         error=str(e).strip())

            logger.error(error)
            raise e

        finally:
            exc_str = self._safe_release_connection()

            # if we had an unhandled exception, re-raise it
            if exc_info and exc_info[1]:
                six.reraise(*exc_info)

            # if releasing failed and the result doesn't have an error yet, set
            # an error
            if exc_str is not None and result.error is None:
                result.error = exc_str
                result.status = 'ERROR'

        result.execution_time = time.time() - started
        return result

    def _safe_release_connection(self):
        """Try to release a connection. If an exception is hit, log and return
        the error string.
        """
        node_name = self.node.name
        try:
            self.adapter.release_connection(node_name)
        except Exception as exc:
            logger.debug(
                'Error releasing connection for node {}: {!s}\n{}'
                .format(node_name, exc, traceback.format_exc())
            )
            return dbt.compat.to_string(exc)

        return None

    def before_execute(self):
        raise NotImplementedException()

    def execute(self, compiled_node, manifest):
        raise NotImplementedException()

    def run(self, compiled_node, manifest):
        return self.execute(compiled_node, manifest)

    def after_execute(self, result):
        raise NotImplementedException()

    def _skip_caused_by_ephemeral_failure(self):
        if self.skip_cause is None or self.skip_cause.node is None:
            return False
        return self.is_ephemeral_model(self.skip_cause.node)

    def on_skip(self):
        schema_name = self.node.schema
        node_name = self.node.name

        error = None
        if not self.is_ephemeral_model(self.node):
            # if this model was skipped due to an upstream ephemeral model
            # failure, print a special 'error skip' message.
            if self._skip_caused_by_ephemeral_failure():
                dbt.ui.printer.print_skip_caused_by_error(
                    self.node,
                    schema_name,
                    node_name,
                    self.node_index,
                    self.num_nodes,
                    self.skip_cause
                )
                # set an error so dbt will exit with an error code
                error = (
                    'Compilation Error in {}, caused by compilation error '
                    'in referenced ephemeral model {}'
                    .format(self.node.unique_id,
                            self.skip_cause.node.unique_id)
                )
            else:
                dbt.ui.printer.print_skip_line(
                    self.node,
                    schema_name,
                    node_name,
                    self.node_index,
                    self.num_nodes
                )

        node_result = RunModelResult(self.node, skip=True, error=error)
        return node_result

    def do_skip(self, cause=None):
        self.skip = True
        self.skip_cause = cause

    @classmethod
    def get_model_schemas(cls, manifest):
        schemas = set()
        for node in manifest.nodes.values():
            if cls.is_refable(node) and not cls.is_ephemeral(node):
                schemas.add(node['schema'])

        return schemas

    @classmethod
    def before_hooks(self, config, adapter, manifest):
        pass

    @classmethod
    def before_run(self, config, adapter, manifest):
        pass

    @classmethod
    def after_run(self, config, adapter, results, manifest):
        pass

    @classmethod
    def after_hooks(self, config, adapter, results, manifest, elapsed):
        pass


class CompileRunner(BaseRunner):
    print_header = False

    def raise_on_first_error(self):
        return True

    def before_execute(self):
        pass

    def after_execute(self, result):
        pass

    def execute(self, compiled_node, manifest):
        return RunModelResult(compiled_node)

    def compile(self, manifest):
        return self._compile_node(self.adapter, self.config, self.node,
                                  manifest, {})

    @classmethod
    def _compile_node(cls, adapter, config, node, manifest, extra_context):
        compiler = dbt.compilation.Compiler(config)
        node = compiler.compile_node(node, manifest, extra_context)
        node = cls._inject_runtime_config(adapter, node, extra_context)

        if(node.injected_sql is not None and
           not (dbt.utils.is_type(node, NodeType.Archive))):
            logger.debug('Writing injected SQL for node "{}"'.format(
                node.unique_id))

            written_path = dbt.writer.write_node(
                node,
                config.target_path,
                'compiled',
                node.injected_sql)

            node.build_path = written_path

        return node

    @classmethod
    def _inject_runtime_config(cls, adapter, node, extra_context):
        wrapped_sql = node.wrapped_sql
        context = cls._node_context(adapter, node)
        context.update(extra_context)
        sql = dbt.clients.jinja.get_rendered(wrapped_sql, context)
        node.wrapped_sql = sql
        return node

    @classmethod
    def _node_context(cls, adapter, node):

        def call_get_columns_in_table(schema_name, table_name):
            return adapter.get_columns_in_table(
                schema_name, table_name, model_name=node.alias
            )

        def call_get_missing_columns(from_schema, from_table,
                                     to_schema, to_table):
            return adapter.get_missing_columns(
                from_schema, from_table, to_schema, to_table, node.alias
            )

        def call_already_exists(schema, table):
            return adapter.already_exists(schema, table, node.alias)

        return {
            "run_started_at": dbt.tracking.active_user.run_started_at,
            "invocation_id": dbt.tracking.active_user.invocation_id,
            "get_columns_in_table": call_get_columns_in_table,
            "get_missing_columns": call_get_missing_columns,
            "already_exists": call_already_exists,
        }


class ModelRunner(CompileRunner):

    def raise_on_first_error(self):
        return False

    @classmethod
    def run_hooks(cls, config, adapter, manifest, hook_type, extra_context):

        nodes = manifest.nodes.values()
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
            compiled = cls._compile_node(adapter, config, hook, manifest,
                                         extra_context)
            statement = compiled.wrapped_sql

            hook_index = hook.get('index', len(hooks))
            hook_dict = dbt.hooks.get_hook_dict(statement, index=hook_index)

            if dbt.flags.STRICT_MODE:
                dbt.contracts.graph.parsed.Hook(**hook_dict)

            sql = hook_dict.get('sql', '')

            if len(sql.strip()) > 0:
                adapter.execute(sql, model_name=model_name, auto_begin=False,
                                fetch=False)

            adapter.release_connection(model_name)

    @classmethod
    def safe_run_hooks(cls, config, adapter, manifest, hook_type,
                       extra_context):
        try:
            cls.run_hooks(config, adapter, manifest, hook_type, extra_context)
        except dbt.exceptions.RuntimeException:
            logger.info("Database error while running {}".format(hook_type))
            raise

    @classmethod
    def create_schemas(cls, config, adapter, manifest):
        required_schemas = cls.get_model_schemas(manifest)

        # Snowflake needs to issue a "use {schema}" query, where schema
        # is the one defined in the profile. Create this schema if it
        # does not exist, otherwise subsequent queries will fail. Generally,
        # dbt expects that this schema will exist anyway.
        required_schemas.add(adapter.get_default_schema())

        existing_schemas = set(adapter.get_existing_schemas())

        for schema in (required_schemas - existing_schemas):
            adapter.create_schema(schema)

    @classmethod
    def populate_adapter_cache(cls, config, adapter, manifest):
        adapter.set_relations_cache(manifest)

    @classmethod
    def before_run(cls, config, adapter, manifest):
        cls.populate_adapter_cache(config, adapter, manifest)
        cls.safe_run_hooks(config, adapter, manifest, RunHookType.Start, {})
        cls.create_schemas(config, adapter, manifest)

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

    @classmethod
    def after_run(cls, config, adapter, results, manifest):
        # in on-run-end hooks, provide the value 'schemas', which is a list of
        # unique schemas that successfully executed models were in
        # errored failed skipped
        schemas = list(set(
            r.node.schema for r in results
            if not any((r.errored, r.failed, r.skipped))
        ))
        cls.safe_run_hooks(config, adapter, manifest, RunHookType.End,
                           {'schemas': schemas, 'results': results})

    @classmethod
    def after_hooks(cls, config, adapter, results, manifest, elapsed):
        cls.print_results_line(results, elapsed)

    def describe_node(self):
        materialization = dbt.utils.get_materialization(self.node)
        schema_name = self.node.schema
        node_name = self.node.alias
        return "{} model {}.{}".format(materialization, schema_name, node_name)

    def print_start_line(self):
        description = self.describe_node()
        dbt.ui.printer.print_start_line(description, self.node_index,
                                        self.num_nodes)

    def print_result_line(self, result):
        schema_name = self.node.schema
        dbt.ui.printer.print_model_result_line(result,
                                               schema_name,
                                               self.node_index,
                                               self.num_nodes)

    def before_execute(self):
        self.print_start_line()

    def after_execute(self, result):
        track_model_run(self.node_index, self.num_nodes, result)
        self.print_result_line(result)

    def execute(self, model, manifest):
        context = dbt.context.runtime.generate(
            model, self.config, manifest)

        materialization_macro = manifest.get_materialization_macro(
            model.get_materialization(),
            self.adapter.type())

        if materialization_macro is None:
            dbt.exceptions.missing_materialization(
                model,
                self.adapter.type())

        materialization_macro.generator(context)()

        # we must have built a new model, add it to the cache
        relation = self.adapter.Relation.create_from_node(self.config, model)
        self.adapter.cache_new_relation(relation)

        result = context['load_result']('main')

        return RunModelResult(model, status=result.status)


class TestRunner(CompileRunner):

    def raise_on_first_error(self):
        return False

    def describe_node(self):
        node_name = self.node.name
        return "test {}".format(node_name)

    def print_result_line(self, result):
        schema_name = self.node.schema
        dbt.ui.printer.print_test_result_line(result,
                                              schema_name,
                                              self.node_index,
                                              self.num_nodes)

    def print_start_line(self):
        description = self.describe_node()
        dbt.ui.printer.print_start_line(description, self.node_index,
                                        self.num_nodes)

    def execute_test(self, test):
        res, table = self.adapter.execute_and_fetch(
            test.wrapped_sql,
            test.name,
            auto_begin=True)

        num_rows = len(table.rows)
        if num_rows > 1:
            num_cols = len(table.columns)
            raise RuntimeError(
                "Bad test {name}: Returned {rows} rows and {cols} cols"
                .format(name=test.get('name'), rows=num_rows, cols=num_cols))

        return table[0][0]

    def before_execute(self):
        self.print_start_line()

    def execute(self, test, manifest):
        status = self.execute_test(test)
        return RunModelResult(test, status=status)

    def after_execute(self, result):
        self.print_result_line(result)


class ArchiveRunner(ModelRunner):

    def raise_on_first_error(self):
        return False

    def describe_node(self):
        cfg = self.node.get('config', {})
        return "archive {source_schema}.{source_table} --> "\
               "{target_schema}.{target_table}".format(**cfg)

    def print_result_line(self, result):
        dbt.ui.printer.print_archive_result_line(result, self.node_index,
                                                 self.num_nodes)


class SeedRunner(ModelRunner):

    def describe_node(self):
        schema_name = self.node.schema
        return "seed file {}.{}".format(schema_name, self.node.alias)

    def before_execute(self):
        description = self.describe_node()
        dbt.ui.printer.print_start_line(description, self.node_index,
                                        self.num_nodes)

    def compile(self, manifest):
        return self.node

    def print_result_line(self, result):
        schema_name = self.node.schema
        dbt.ui.printer.print_seed_result_line(result,
                                              schema_name,
                                              self.node_index,
                                              self.num_nodes)
