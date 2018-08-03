
from dbt.logger import GLOBAL_LOGGER as logger
from dbt.exceptions import NotImplementedException
from dbt.utils import get_nodes_by_tags
from dbt.node_types import NodeType, RunHookType
from dbt.adapters.factory import get_adapter

import dbt.clients.jinja
import dbt.context.runtime
import dbt.utils
import dbt.tracking
import dbt.ui.printer
import dbt.flags
import dbt.schema
import dbt.templates
import dbt.writer

import time


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


class RunModelResult(object):
    def __init__(self, node, error=None, skip=False, status=None,
                 failed=None, execution_time=0):
        self.node = node
        self.error = error
        self.skip = skip
        self.fail = failed
        self.status = status
        self.execution_time = execution_time

    @property
    def errored(self):
        return self.error is not None

    @property
    def failed(self):
        return self.fail

    @property
    def skipped(self):
        return self.skip


class RunOperationResult(RunModelResult):
    def __init__(self, node, error=None, skip=False, status=None,
                 failed=None, execution_time=0, returned=None):
        super(RunOperationResult, self).__init__(node, error, skip, status,
                                                 failed, execution_time)
        self.returned = returned


class BaseRunner(object):
    print_header = True

    def __init__(self, project, adapter, node, node_index, num_nodes):
        self.project = project
        self.profile = project.run_environment()
        self.adapter = adapter
        self.node = node
        self.node_index = node_index
        self.num_nodes = num_nodes

        self.skip = False

    def raise_on_first_error(self):
        return False

    @classmethod
    def is_refable(cls, node):
        return node.get('resource_type') in NodeType.refable()

    @classmethod
    def is_ephemeral(cls, node):
        return dbt.utils.get_materialization(node) == 'ephemeral'

    @classmethod
    def is_ephemeral_model(cls, node):
        return cls.is_refable(node) and cls.is_ephemeral(node)

    def safe_run(self, flat_graph):
        catchable_errors = (dbt.exceptions.CompilationException,
                            dbt.exceptions.RuntimeException)

        result = RunModelResult(self.node)
        started = time.time()

        try:
            # if we fail here, we still have a compiled node to return
            # this has the benefit of showing a build path for the errant model
            compiled_node = self.compile(flat_graph)
            result.node = compiled_node

            # for ephemeral nodes, we only want to compile, not run
            if not self.is_ephemeral_model(self.node):
                result = self.run(compiled_node, flat_graph)

        except catchable_errors as e:
            if e.node is None:
                e.node = result.node

            result.error = dbt.compat.to_string(e)
            result.status = 'ERROR'

        except dbt.exceptions.InternalException as e:
            build_path = self.node.get('build_path')
            prefix = 'Internal error executing {}'.format(build_path)

            error = "{prefix}\n{error}\n\n{note}".format(
                         prefix=dbt.ui.printer.red(prefix),
                         error=str(e).strip(),
                         note=INTERNAL_ERROR_STRING)
            logger.debug(error)

            result.error = dbt.compat.to_string(e)
            result.status = 'ERROR'

        except Exception as e:
            prefix = "Unhandled error while executing {filepath}".format(
                        filepath=self.node.get('build_path'))

            error = "{prefix}\n{error}".format(
                         prefix=dbt.ui.printer.red(prefix),
                         error=str(e).strip())

            logger.debug(error)
            raise e

        finally:
            node_name = self.node.get('name')
            self.adapter.release_connection(self.profile, node_name)

        result.execution_time = time.time() - started
        return result

    def before_execute(self):
        raise NotImplementedException()

    def execute(self, compiled_node, flat_graph):
        raise NotImplementedException()

    def run(self, compiled_node, flat_graph):
        return self.execute(compiled_node, flat_graph)

    def after_execute(self, result):
        raise NotImplementedException()

    def on_skip(self):
        schema_name = self.node.get('schema')
        node_name = self.node.get('name')

        if not self.is_ephemeral_model(self.node):
            dbt.ui.printer.print_skip_line(self.node, schema_name, node_name,
                                           self.node_index, self.num_nodes)

        node_result = RunModelResult(self.node, skip=True)
        return node_result

    def do_skip(self):
        self.skip = True

    @classmethod
    def get_model_schemas(cls, flat_graph):
        schemas = set()
        for node in flat_graph['nodes'].values():
            if cls.is_refable(node) and not cls.is_ephemeral(node):
                schemas.add(node['schema'])

        return schemas

    @classmethod
    def before_hooks(self, project, adapter, flat_graph):
        pass

    @classmethod
    def before_run(self, project, adapter, flat_graph):
        pass

    @classmethod
    def after_run(self, project, adapter, results, flat_graph):
        pass

    @classmethod
    def after_hooks(self, project, adapter, results, flat_graph, elapsed):
        pass


class CompileRunner(BaseRunner):
    print_header = False

    def raise_on_first_error(self):
        return True

    def before_execute(self):
        pass

    def after_execute(self, result):
        pass

    def execute(self, compiled_node, flat_graph):
        return RunModelResult(compiled_node)

    def compile(self, flat_graph):
        return self._compile_node(self.adapter, self.project, self.node,
                                  flat_graph)

    @classmethod
    def _compile_node(cls, adapter, project, node, flat_graph):
        compiler = dbt.compilation.Compiler(project)
        node = compiler.compile_node(node, flat_graph)
        node = cls._inject_runtime_config(adapter, project, node)

        if(node['injected_sql'] is not None and
           not (dbt.utils.is_type(node, NodeType.Archive))):
            logger.debug('Writing injected SQL for node "{}"'.format(
                node['unique_id']))

            written_path = dbt.writer.write_node(
                node,
                project.get('target-path'),
                'compiled',
                node['injected_sql'])

            node['build_path'] = written_path

        return node

    @classmethod
    def _inject_runtime_config(cls, adapter, project, node):
        wrapped_sql = node.get('wrapped_sql')
        context = cls._node_context(adapter, project, node)
        sql = dbt.clients.jinja.get_rendered(wrapped_sql, context)
        node['wrapped_sql'] = sql
        return node

    @classmethod
    def _node_context(cls, adapter, project, node):
        profile = project.run_environment()

        def call_get_columns_in_table(schema_name, table_name):
            return adapter.get_columns_in_table(
                profile, project, schema_name,
                table_name, model_name=node.get('alias'))

        def call_get_missing_columns(from_schema, from_table,
                                     to_schema, to_table):
            return adapter.get_missing_columns(
                profile, project, from_schema, from_table,
                to_schema, to_table, node.get('alias'))

        def call_already_exists(schema, table):
            return adapter.already_exists(
                profile, project, schema, table, node.get('alias'))

        return {
            "run_started_at": dbt.tracking.active_user.run_started_at,
            "invocation_id": dbt.tracking.active_user.invocation_id,
            "get_columns_in_table": call_get_columns_in_table,
            "get_missing_columns": call_get_missing_columns,
            "already_exists": call_already_exists,
        }

    @classmethod
    def create_schemas(cls, project, adapter, flat_graph):
        profile = project.run_environment()
        required_schemas = cls.get_model_schemas(flat_graph)
        existing_schemas = set(adapter.get_existing_schemas(profile, project))
        for schema in (required_schemas - existing_schemas):
            adapter.create_schema(profile, project, schema)


class ModelRunner(CompileRunner):

    def raise_on_first_error(self):
        return False

    @classmethod
    def run_hooks(cls, project, adapter, flat_graph, hook_type):
        profile = project.run_environment()

        nodes = flat_graph.get('nodes', {}).values()
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
            adapter.clear_transaction(profile, model_name)
            compiled = cls._compile_node(adapter, project, hook, flat_graph)
            statement = compiled['wrapped_sql']

            hook_index = hook.get('index', len(hooks))
            hook_dict = dbt.hooks.get_hook_dict(statement, index=hook_index)

            if dbt.flags.STRICT_MODE:
                dbt.contracts.graph.parsed.Hook(**hook_dict)

            sql = hook_dict.get('sql', '')

            if len(sql.strip()) > 0:
                adapter.execute(profile, sql, model_name=model_name,
                                auto_begin=False, fetch=False)

            adapter.release_connection(profile, model_name)

    @classmethod
    def safe_run_hooks(cls, project, adapter, flat_graph, hook_type):
        try:
            cls.run_hooks(project, adapter, flat_graph, hook_type)

        except dbt.exceptions.RuntimeException:
            logger.info("Database error while running {}".format(hook_type))
            raise

    @classmethod
    def create_schemas(cls, project, adapter, flat_graph):
        profile = project.run_environment()
        required_schemas = cls.get_model_schemas(flat_graph)

        # Snowflake needs to issue a "use {schema}" query, where schema
        # is the one defined in the profile. Create this schema if it
        # does not exist, otherwise subsequent queries will fail. Generally,
        # dbt expects that this schema will exist anyway.
        required_schemas.add(adapter.get_default_schema(profile, project))

        existing_schemas = set(adapter.get_existing_schemas(profile, project))

        for schema in (required_schemas - existing_schemas):
            adapter.create_schema(profile, project, schema)

    @classmethod
    def before_run(cls, project, adapter, flat_graph):
        cls.safe_run_hooks(project, adapter, flat_graph, RunHookType.Start)
        cls.create_schemas(project, adapter, flat_graph)

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
    def after_run(cls, project, adapter, results, flat_graph):
        cls.safe_run_hooks(project, adapter, flat_graph, RunHookType.End)

    @classmethod
    def after_hooks(cls, project, adapter, results, flat_graph, elapsed):
        cls.print_results_line(results, elapsed)

    def describe_node(self):
        materialization = dbt.utils.get_materialization(self.node)
        schema_name = self.node.get('schema')
        node_name = self.node.get('alias')
        return "{} model {}.{}".format(materialization, schema_name, node_name)

    def print_start_line(self):
        description = self.describe_node()
        dbt.ui.printer.print_start_line(description, self.node_index,
                                        self.num_nodes)

    def print_result_line(self, result):
        schema_name = self.node.get('schema')
        dbt.ui.printer.print_model_result_line(result,
                                               schema_name,
                                               self.node_index,
                                               self.num_nodes)

    def before_execute(self):
        self.print_start_line()

    def after_execute(self, result):
        track_model_run(self.node_index, self.num_nodes, result)
        self.print_result_line(result)

    def execute(self, model, flat_graph):
        context = dbt.context.runtime.generate(
            model, self.project.cfg, flat_graph)

        materialization_macro = dbt.utils.get_materialization_macro(
            flat_graph,
            dbt.utils.get_materialization(model),
            self.adapter.type())

        if materialization_macro is None:
            dbt.exceptions.missing_materialization(
                model,
                self.adapter.type())

        materialization_macro.generator(context)()

        result = context['load_result']('main')

        return RunModelResult(model, status=result.status)


class TestRunner(CompileRunner):

    def raise_on_first_error(self):
        return False

    def describe_node(self):
        node_name = self.node.get('name')
        return "test {}".format(node_name)

    def print_result_line(self, result):
        schema_name = self.node.get('schema')
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
            self.profile,
            test.get('wrapped_sql'),
            test.get('name'),
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

    def execute(self, test, flat_graph):
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
        schema_name = self.node.get('schema')
        return "seed file {}.{}".format(schema_name, self.node['alias'])

    def before_execute(self):
        description = self.describe_node()
        dbt.ui.printer.print_start_line(description, self.node_index,
                                        self.num_nodes)

    def compile(self, flat_graph):
        return self.node

    def print_result_line(self, result):
        schema_name = self.node.get('schema')
        dbt.ui.printer.print_seed_result_line(result,
                                              schema_name,
                                              self.node_index,
                                              self.num_nodes)
