
from dbt.logger import GLOBAL_LOGGER as logger
from dbt.exceptions import NotImplementedException
from dbt.utils import get_nodes_by_tags
from dbt.node_types import NodeType, RunHookType

import dbt.utils
import dbt.tracking
import dbt.ui.printer
import dbt.flags
import dbt.schema

import dbt.clients.jinja
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
        "run_error": run_model_result.error,
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

    def is_ephemeral(self):
        return dbt.utils.get_materialization(self.node) == 'ephemeral'

    def safe_run(self, flat_graph, existing):
        catchable_errors = (dbt.exceptions.CompilationException,
                            dbt.exceptions.RuntimeException,
                            dbt.exceptions.ProgrammingException)

        result = RunModelResult(self.node)
        started = time.time()

        try:
            # if we fail here, we still have a compiled node to return
            # this has the benefit of showing a build path for the errant model
            compiled_node = self.compile(flat_graph)
            result.node = compiled_node

            # for ephemeral nodes, we only want to compile, not run
            if not self.is_ephemeral():
                result = self.run(compiled_node, existing)

        except catchable_errors as e:
            result.error = str(e).strip()
            result.status = 'ERROR'

        except dbt.exceptions.InternalException as e:
            build_path = self.node.get('build_path')
            prefix = 'Internal error executing {}'.format(build_path)

            error = "{prefix}\n{error}\n\n{note}".format(
                         prefix=dbt.ui.printer.red(prefix),
                         error=str(e).strip(),
                         note=INTERNAL_ERROR_STRING)
            logger.debug(error)

            result.error = str(e).strip()
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

    @classmethod
    def get_schema(cls, adapter, profile):
        return adapter.get_default_schema(profile)

    def before_execute(self):
        raise NotImplementedException()

    def execute(self, compiled_node, existing):
        raise NotImplementedException()

    def run(self, compiled_node, existing):
        return self.execute(compiled_node, existing)

    def after_execute(self, result):
        raise NotImplementedException()

    def on_skip(self):
        schema_name = self.get_schema(self.adapter, self.profile)

        node_name = self.node.get('name')
        dbt.ui.printer.print_skip_line(self.node, schema_name, node_name,
                                       self.node_index, self.num_nodes)

        node_result = RunModelResult(self.node, skip=True)
        return node_result

    def do_skip(self):
        self.skip = True

    @classmethod
    def before_run(self, project, adapter, flat_graph):
        pass

    @classmethod
    def after_run(self, project, adapter, results, flat_graph, elapsed):
        pass


class CompileRunner(BaseRunner):
    print_header = False

    def raise_on_first_error(self):
        return True

    def before_execute(self):
        pass

    def after_execute(self, result):
        pass

    def execute(self, compiled_node, existing):
        return RunModelResult(compiled_node)

    def compile(self, flat_graph):
        return self.compile_node(self.adapter, self.project, self.node,
                                 flat_graph)

    @classmethod
    def compile_node(cls, adapter, project, node, flat_graph):
        compiler = dbt.compilation.Compiler(project)
        node = compiler.compile_node(node, flat_graph)
        node = cls.inject_runtime_config(adapter, project, node)

        return node

    @classmethod
    def inject_runtime_config(cls, adapter, project, node):
        wrapped_sql = node.get('wrapped_sql')
        context = cls.node_context(adapter, project, node)
        sql = dbt.clients.jinja.get_rendered(wrapped_sql, context)
        node['wrapped_sql'] = sql
        return node

    @classmethod
    def node_context(cls, adapter, project, node):
        profile = project.run_environment()

        def call_get_columns_in_table(schema_name, table_name):
            return adapter.get_columns_in_table(
                profile, schema_name, table_name, node.get('name'))

        def call_get_missing_columns(from_schema, from_table,
                                     to_schema, to_table):
            return adapter.get_missing_columns(
                profile, from_schema, from_table,
                to_schema, to_table, node.get('name'))

        def call_table_exists(schema, table):
            return adapter.table_exists(
                profile, schema, table, node.get('name'))

        return {
            "run_started_at": dbt.tracking.active_user.run_started_at,
            "invocation_id": dbt.tracking.active_user.invocation_id,
            "get_columns_in_table": call_get_columns_in_table,
            "get_missing_columns": call_get_missing_columns,
            "already_exists": call_table_exists,
        }


class ModelRunner(CompileRunner):

    def raise_on_first_error(self):
        return False

    @classmethod
    def try_create_schema(cls, project, adapter):
        profile = project.run_environment()
        schema_name = cls.get_schema(adapter, profile)

        schema_exists = adapter.check_schema_exists(profile, schema_name)

        if schema_exists:
            logger.debug('schema {} already exists -- '
                         'not creating'.format(schema_name))
            return

        adapter.create_schema(profile, schema_name)

    @classmethod
    def run_hooks(cls, project, adapter, flat_graph, hook_type):
        profile = project.run_environment()

        nodes = flat_graph.get('nodes', {}).values()
        hooks = get_nodes_by_tags(nodes, {hook_type}, NodeType.Operation)

        for hook in hooks:
            compiled = cls.compile_node(adapter, project, hook, flat_graph)
            model_name = compiled.get('name')
            sql = compiled['wrapped_sql']
            adapter.execute_one(profile, sql, model_name=model_name,
                                auto_begin=True)
            adapter.commit_if_has_connection(profile, model_name)

    @classmethod
    def safe_run_hooks(cls, project, adapter, flat_graph, hook_type):
        try:
            cls.run_hooks(project, adapter, flat_graph, hook_type)
        except dbt.exceptions.RuntimeException as e:
            logger.info("Database error while running {}".format(hook_type))
            raise

    @classmethod
    def before_run(cls, project, adapter, flat_graph):
        cls.try_create_schema(project, adapter)
        cls.safe_run_hooks(project, adapter, flat_graph, RunHookType.Start)

    @classmethod
    def print_results_line(cls, results, execution_time):
        nodes = [r.node for r in results]
        stat_line = dbt.ui.printer.get_counts(nodes)

        dbt.ui.printer.print_timestamped_line("")
        dbt.ui.printer.print_timestamped_line(
            "Finished running {stat_line} in {execution_time:0.2f}s."
            .format(stat_line=stat_line, execution_time=execution_time))

    @classmethod
    def after_run(cls, project, adapter, results, flat_graph, elapsed):
        cls.safe_run_hooks(project, adapter, flat_graph, RunHookType.End)
        cls.print_results_line(results, elapsed)

    def describe_node(self):
        materialization = dbt.utils.get_materialization(self.node)
        schema_name = self.get_schema(self.adapter, self.profile)
        node_name = self.node.get('name')

        return "{} model {}.{}".format(materialization, schema_name, node_name)

    def print_start_line(self):
        description = self.describe_node()
        dbt.ui.printer.print_start_line(description, self.node_index,
                                        self.num_nodes)

    def print_result_line(self, result):
        schema_name = self.get_schema(self.adapter, self.profile)
        dbt.ui.printer.print_model_result_line(result,
                                               schema_name,
                                               self.node_index,
                                               self.num_nodes)

    def before_execute(self):
        self.print_start_line()

    def after_execute(self, result):
        track_model_run(self.node_index, self.num_nodes, result)
        self.print_result_line(result)

    def execute(self, model, existing):
        materializer = self.adapter.get_materializer(model, existing)
        status = materializer.materialize(self.profile)

        return RunModelResult(model, status=status)


class TestRunner(CompileRunner):

    def raise_on_first_error(self):
        return False

    def describe_node(self):
        node_name = self.node.get('name')
        return "test {}".format(node_name)

    def print_result_line(self, result):
        schema_name = self.get_schema(self.adapter, self.profile)
        dbt.ui.printer.print_test_result_line(result,
                                              schema_name,
                                              self.node_index,
                                              self.num_nodes)

    def print_start_line(self):
        description = self.describe_node()
        dbt.ui.printer.print_start_line(description, self.node_index,
                                        self.num_nodes)

    def execute_test(self, test):
        rows = self.adapter.execute_and_fetch(
            self.profile,
            test.get('wrapped_sql'),
            test.get('name'),
            auto_begin=True)

        num_rows = len(rows)
        if num_rows > 1:
            num_cols = len(rows[0])
            raise RuntimeError(
                "Bad test {name}: Returned {rows} rows and {cols} cols"
                .format(name=test.name, rows=num_rows, cols=num_cols))

        return rows[0][0]

    def before_execute(self):
        self.print_start_line()

    def execute(self, test, existing):
        status = self.execute_test(test)
        return RunModelResult(test, status=status)

    def after_execute(self, result):
        self.print_result_line(result)


class ArchiveRunner(CompileRunner):

    def raise_on_first_error(self):
        return False

    def describe_node(self):
        cfg = self.node.get('config', {})
        return "archive {source_schema}.{source_table} --> "\
               "{target_schema}.{target_table}".format(**cfg)

    def print_result_line(self, result):
        dbt.ui.printer.print_archive_result_line(result, self.node_index,
                                                 self.num_nodes)

    def print_start_line(self):
        description = self.describe_node()
        dbt.ui.printer.print_start_line(description, self.node_index,
                                        self.num_nodes)

    def before_execute(self):
        self.print_start_line()

    def after_execute(self, result):
        self.print_result_line(result)

    def execute(self, archive, existing):
        status = self.execute_archive()
        return RunModelResult(archive, status=status)

    def execute_archive(self):
        node = self.node
        node_cfg = node.get('config', {})

        context = self.node_context(self.adapter, self.project, self.node)

        source_schema = node_cfg.get('source_schema')
        source_table = node_cfg.get('source_table')

        source_columns = self.adapter.get_columns_in_table(self.profile,
                                                           source_schema,
                                                           source_table)

        if len(source_columns) == 0:
            raise RuntimeError(
                'Source table "{}"."{}" does not '
                'exist'.format(source_schema, source_table))

        dest_columns = source_columns + [
            dbt.schema.Column("valid_from", "timestamp", None),
            dbt.schema.Column("valid_to", "timestamp", None),
            dbt.schema.Column("scd_id", "text", None),
            dbt.schema.Column("dbt_updated_at", "timestamp", None)
        ]

        self.adapter.create_table(
            self.profile,
            schema=node_cfg.get('target_schema'),
            table=node_cfg.get('target_table'),
            columns=dest_columns,
            sort='dbt_updated_at',
            dist='scd_id',
            model_name=node.get('name'))

        # TODO move this to inject_runtime_config, generate archive SQL
        # in wrap step. can't do this right now because we actually need
        # to inspect status of the schema at runtime and archive requires
        # a lot of information about the schema to generate queries.
        template_ctx = context.copy()
        template_ctx.update(node_cfg)

        template = dbt.templates.SCDArchiveTemplate
        select = dbt.clients.jinja.get_rendered(template, template_ctx)

        insert_stmt = dbt.templates.ArchiveInsertTemplate().wrap(
            schema=node_cfg.get('target_schema'),
            table=node_cfg.get('target_table'),
            query=select,
            unique_key=node_cfg.get('unique_key'))

        node['wrapped_sql'] = dbt.clients.jinja.get_rendered(insert_stmt,
                                                             template_ctx)

        result = self.adapter.execute_model(
            profile=self.profile,
            model=node)

        self.adapter.commit_if_has_connection(self.profile, node.get('name'))
        return result
