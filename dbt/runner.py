import os
import time

from dbt.adapters.factory import get_adapter
from dbt.logger import GLOBAL_LOGGER as logger
from dbt.node_runners import RunModelResult

from dbt.utils import get_materialization, NodeType, is_type, get_nodes_by_tags

import dbt.clients.jinja
import dbt.compilation
import dbt.exceptions
import dbt.linker
import dbt.tracking
import dbt.schema
import dbt.model
import dbt.ui.printer


from  dbt.graph.selector import NodeSelector, FlatNodeSelector

from multiprocessing.dummy import Pool as ThreadPool


INTERNAL_ERROR_STRING = """This is an error in dbt. Please try again. If \
the error persists, open an issue at https://github.com/fishtown-analytics/dbt
""".strip()


def is_enabled(model):
    return model.get('config', {}).get('enabled') is True


@contextmanager
def model_error_handler(profile, adapter, node, run_model_result):
    catchable_errors = (dbt.exceptions.CompilationException,
                        dbt.exceptions.RuntimeException)

    try:
        yield

    except catchable_errors as e:
        run_model_result.error = str(e)
        run_model_result.status = 'ERROR'

    except dbt.exceptions.InternalException as e:
        build_path = node.get('build_path')
        prefix = 'Internal error executing {}'.format(build_path)

        error = "{prefix}\n{error}\n\n{note}".format(
                     prefix=dbt.ui.printer.red(prefix),
                     error=str(e).strip(),
                     note=INTERNAL_ERROR_STRING)
        logger.debug(error)

        run_model_result.error = str(e)
        run_model_result.status = 'ERROR'

    except Exception as e:
        prefix = "Unhandled error while executing {filepath}".format(
                    filepath=node.get('build_path'))

        error = "{prefix}\n{error}".format(
                     prefix=dbt.ui.printer.red(prefix),
                     error=str(e).strip())

        logger.debug(error)
        raise e

    finally:
        adapter.release_connection(profile, node.get('name'))

def print_result_line(result, schema, index, total):
    node = result.node

    if is_type(node, NodeType.Model):
        dbt.ui.printer.print_model_result_line(result, schema, index, total)
    elif is_type(node, NodeType.Test):
        dbt.ui.printer.print_test_result_line(result, schema, index, total)
    elif is_type(node, NodeType.Archive):
        dbt.ui.printer.print_archive_result_line(result, index, total)


def execute_test(profile, test):
    adapter = get_adapter(profile)
    handle, cursor = adapter.execute_one(
        profile,
        test.get('wrapped_sql'),
        test.get('name'))

    rows = cursor.fetchall()

    if len(rows) > 1:
        raise RuntimeError(
            "Bad test {name}: Returned {num_rows} rows instead of 1"
            .format(name=test.name, num_rows=len(rows)))

    row = rows[0]
    if len(row) > 1:
        raise RuntimeError(
            "Bad test {name}: Returned {num_cols} cols instead of 1"
            .format(name=test.name, num_cols=len(row)))

    return row[0]


def execute_model(profile, model, existing):
    adapter = get_adapter(profile)
    schema = adapter.get_default_schema(profile)

    tmp_name = '{}__dbt_tmp'.format(model.get('name'))

    if dbt.flags.NON_DESTRUCTIVE or profile.get('type') == 'bigquery': # TODO
        # for non destructive mode, we only look at the already existing table.
        tmp_name = model.get('name')

    result = None

    # TRUNCATE / DROP
    if get_materialization(model) == 'table' and \
       dbt.flags.NON_DESTRUCTIVE and \
       existing.get(tmp_name) == 'table':
        # tables get truncated instead of dropped in non-destructive mode.
        adapter.truncate(
            profile=profile,
            table=tmp_name,
            model_name=model.get('name'))

    elif dbt.flags.NON_DESTRUCTIVE:
        # never drop existing relations in non destructive mode.
        pass

    elif (get_materialization(model) != 'incremental' and
          existing.get(tmp_name) is not None):
        # otherwise, for non-incremental things, drop them with IF EXISTS
        adapter.drop(
            profile=profile,
            relation=tmp_name,
            relation_type=existing.get(tmp_name),
            model_name=model.get('name'))

        # and update the list of what exists
        existing = adapter.query_for_existing(
            profile,
            schema,
            model_name=model.get('name'))

    # EXECUTE
    if get_materialization(model) == 'view' and dbt.flags.NON_DESTRUCTIVE and \
       model.get('name') in existing:
        # views don't need to be recreated in non destructive mode since they
        # will repopulate automatically. note that we won't run DDL for these
        # views either.
        pass
    elif is_enabled(model) and get_materialization(model) != 'ephemeral':
        result = adapter.execute_model(profile, model)

    # DROP OLD RELATION AND RENAME
    if dbt.flags.NON_DESTRUCTIVE:
        # in non-destructive mode, we truncate and repopulate tables, and
        # don't modify views.
        pass
    elif get_materialization(model) in ['table', 'view']:
        # otherwise, drop tables and views, and rename tmp tables/views to
        # their new names
        if existing.get(model.get('name')) is not None:
            adapter.drop(
                profile=profile,
                relation=model.get('name'),
                relation_type=existing.get(model.get('name')),
                model_name=model.get('name'))

        adapter.rename(profile=profile,
                       from_name=tmp_name,
                       to_name=model.get('name'),
                       model_name=model.get('name'))

    return result


def execute_archive(profile, node, context):
    adapter = get_adapter(profile)

    node_cfg = node.get('config', {})

    source_columns = adapter.get_columns_in_table(
        profile, node_cfg.get('source_schema'), node_cfg.get('source_table'))

    if len(source_columns) == 0:
        source_schema = node_cfg.get('source_schema')
        source_table = node_cfg.get('source_table')
        raise RuntimeError(
            'Source table "{}"."{}" does not '
            'exist'.format(source_schema, source_table))

    dest_columns = source_columns + [
        dbt.schema.Column("valid_from", "timestamp", None),
        dbt.schema.Column("valid_to", "timestamp", None),
        dbt.schema.Column("scd_id", "text", None),
        dbt.schema.Column("dbt_updated_at", "timestamp", None)
    ]

    adapter.create_table(
        profile,
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

    select = dbt.clients.jinja.get_rendered(dbt.templates.SCDArchiveTemplate,
                                            template_ctx)

    insert_stmt = dbt.templates.ArchiveInsertTemplate().wrap(
        schema=node_cfg.get('target_schema'),
        table=node_cfg.get('target_table'),
        query=select,
        unique_key=node_cfg.get('unique_key'))

    node['wrapped_sql'] = dbt.clients.jinja.get_rendered(insert_stmt,
                                                         template_ctx)

    result = adapter.execute_model(
        profile=profile,
        model=node)

    return result


class RunManager(object):
    def __init__(self, project, target_path, args):
        self.project = project
        self.target_path = target_path
        self.args = args

        profile = self.project.run_environment()

        # TODO validate the number of threads
        if self.args.threads is None:
            self.threads = profile.get('threads', 1)
        else:
            self.threads = self.args.threads

    def node_context(self, node):
        profile = self.project.run_environment()
        adapter = get_adapter(profile)

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

    def inject_runtime_config(self, node):
        sql = dbt.clients.jinja.get_rendered(node.get('wrapped_sql'),
                                             self.node_context(node))

        node['wrapped_sql'] = sql

        return node

    def deserialize_graph(self):
        logger.info("Loading dependency graph file.")

        base_target_path = self.project['target-path']
        graph_file = os.path.join(
            base_target_path,
            dbt.compilation.graph_file_name
        )

        return dbt.linker.from_file(graph_file)

    def execute_node(self, node, flat_graph, existing, profile, adapter):
        result = None

        logger.debug("executing node %s", node.get('unique_id'))

        if node.get('skip') is True:
            return "SKIP"

        node = self.inject_runtime_config(node)

        if is_type(node, NodeType.Model):
            result = execute_model(profile, node, existing)
        elif is_type(node, NodeType.Test):
            result = execute_test(profile, node)
        elif is_type(node, NodeType.Archive):
            result = execute_archive(
                profile, node, self.node_context(node))

        adapter.commit_if_has_connection(profile, node.get('name'))

        return node, result

    def safe_execute_node(self, data):
        node = data['node']
        flat_graph = data['flat_graph']
        existing = data['existing']
        schema_name = data['schema_name']
        node_index = data['node_index']
        num_nodes = data['num_nodes']

        start_time = time.time()

        error = None
        status = None
        is_ephemeral = (get_materialization(node) == 'ephemeral')

        if not is_ephemeral:
            print_start_line(node,
                             schema_name,
                             node_index,
                             num_nodes)

        profile = self.project.run_environment()
        adapter = get_adapter(profile)

        node = self.compile_node(node, flat_graph)

        if not is_ephemeral:
            node, status = self.execute_node(node, flat_graph, existing,
                                             profile, adapter)

        # ---

        execution_time = time.time() - start_time

        result = RunModelResult(node,
                                error=error,
                                status=status,
                                execution_time=execution_time)

        if not is_ephemeral:
            print_result_line(result, schema_name, node_index, num_nodes)

        return result

    def on_model_failure(self, linker, selected_nodes):
        def skip_dependent(node):
            dependent_nodes = linker.get_dependent_nodes(node.get('unique_id'))
            for node in dependent_nodes:
                if node in selected_nodes:
                    node_data = linker.get_node(node)
                    node_data['skip'] = True
                    linker.update_node_data(node, node_data)

        return skip_dependent

    def get_runners(self, Runner, adapter, node_dependency_list):
        all_nodes = dbt.utils.flatten_nodes(node_dependency_list)
        nodes = [n for n in all_nodes if get_materialization(n) != 'ephemeral']
        num_nodes = len(nodes)

        node_runners = {}
        for i, node in enumerate(nodes):
            uid = node.get('unique_id')
            runner = Runner(adapter, node, i + 1, num_nodes)
            node_runners[uid] = runner

        return node_runners

    def call_runner(self, data):
        runner = data['runner']
        existing = data['existing']

        node = runner.node
        adapter = runner.adapter
        profile = self.project.run_environment()
        # mtutable - this is set in context manager
        error_result = RunModelResult(node)

        result = None
        with model_error_handler(profile, adapter, node, error_result):
            result = runner.execute(self.project, existing)

        if result is None:
            return error_result
        else:
            return return

    def execute_nodes(self, Runner, flat_graph, node_dependency_list):
        profile = self.project.run_environment()
        adapter = get_adapter(profile)
        schema_name = adapter.get_default_schema(profile)

        num_threads = self.threads
        logger.info("Concurrency: {} threads (target='{}')".format(
            num_threads, self.project.get_target().get('name'))
        )

        existing = adapter.query_for_existing(profile, schema_name)
        node_runners = self.get_runners(Runner, adapter, node_dependency_list)

        pool = ThreadPool(num_threads)
        node_results = []
        for node_list in node_dependency_list:
            runners = [node_runners[n.get('unique_id')] for n in node_list]

            args_list = [{'runner': runner, 'existing': existing} for runner in runners] # noqa

            try:
                for result in pool.imap_unordered(self.call_runner, args_list):
                    node_results.append(result)

                    # propagate so that CTEs get injected properly
                    # TODO : is how does this work now????? Did we pass flat_graph by reference?
                    # is that terrible? Will this just work??
                    node_id = result.node.get('unique_id')
                    flat_graph['nodes'][node_id] = result.node

                    if result.errored:
                        on_failure(result.node)
                        logger.info(result.error)

            except KeyboardInterrupt:
                pool.close()
                pool.terminate()

                profile = self.project.run_environment()
                adapter = get_adapter(profile)

                for conn_name in adapter.cancel_open_connections(profile):
                    dbt.ui.printer.print_cancel_line(conn_name, schema_name)

                pool.join()
                raise

        pool.close()
        pool.join()

        return node_results

    def run_from_graph(self, Selector, Runner, query):
        compiler = dbt.compilation.Compiler(self.project)
        compiler.initialize()
        (flat_graph, linker) = compiler.compile()

        selector = Selector(linker, flat_graph)
        selected_nodes = selector.select(query)
        dependency_list = selector.as_node_list(selected_nodes)

        profile = self.project.run_environment()
        adapter = get_adapter(profile)

        flat_nodes = dbt.utils.flatten_nodes(dependency_list)
        if len(flat_nodes) == 0:
            logger.info("WARNING: Nothing to do. Try checking your model "
                        "configs and model specification args")
            return []
        else:
            stat_line = dbt.ui.printer.get_counts(flat_nodes)
            full_line = "{} {}".format(Runner.verb, stat_line)

            logger.info("")
            dbt.ui.printer.print_timestamped_line(full_line)
            dbt.ui.printer.print_timestamped_line("")

        try:
            Runner.before_run(self.project, adapter, flat_graph)
            started = time.time()
            results = self.execute_nodes(Runner, flat_graph, dependency_list)
            elapsed = time.time() - start_time
            Runner.after_run(self.project, adapter, results, elapsed)

        finally:
            adapter.cleanup_connections()

        return results

    # ------------------------------------

    def run(self, query, Runner):
        Selector = NodeSelector
        return self.run_from_graph(Selector, Runner, query)

    def run_flat(self, query, Runner):
        Selector = FlatNodeSelector
        return self.run_from_graph(Selector, Runner, query)
