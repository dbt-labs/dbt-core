import hashlib
import psycopg2
import os
import time
import itertools

from dbt.adapters.factory import get_adapter
from dbt.logger import GLOBAL_LOGGER as logger

from dbt.utils import get_materialization, NodeType, is_type, get_nodes_by_tags

import dbt.clients.jinja
import dbt.compilation
import dbt.exceptions
import dbt.linker
import dbt.tracking
import dbt.schema
import dbt.graph.selector
import dbt.model
import dbt.ui.printer

from multiprocessing.dummy import Pool as ThreadPool


ABORTED_TRANSACTION_STRING = ("current transaction is aborted, commands "
                              "ignored until end of transaction block")

INTERNAL_ERROR_STRING = """This is an error in dbt. Please try again. If \
the error persists, open an issue at https://github.com/fishtown-analytics/dbt
""".strip()


def get_hash(model):
    return hashlib.md5(model.get('unique_id').encode('utf-8')).hexdigest()


def get_hashed_contents(model):
    return hashlib.md5(model.get('raw_sql').encode('utf-8')).hexdigest()


def is_enabled(model):
    return model.get('config', {}).get('enabled') is True


def print_start_line(node, schema, index, total):
    if is_type(node, NodeType.Model):
        dbt.ui.printer.print_model_start_line(node, schema, index, total)
    if is_type(node, NodeType.Test):
        dbt.ui.printer.print_test_start_line(node, schema, index, total)
    if is_type(node, NodeType.Archive):
        dbt.ui.printer.print_archive_start_line(node, index, total)


def print_result_line(result, schema, index, total):
    node = result.node

    if is_type(node, NodeType.Model):
        dbt.ui.printer.print_model_result_line(result, schema, index, total)
    elif is_type(node, NodeType.Test):
        dbt.ui.printer.print_test_result_line(result, schema, index, total)
    elif is_type(node, NodeType.Archive):
        dbt.ui.printer.print_archive_result_line(result, index, total)


def print_results_line(results, execution_time):
    nodes = [r.node for r in results]
    stat_line = dbt.ui.printer.get_counts(nodes)

    dbt.ui.printer.print_timestamped_line("")
    dbt.ui.printer.print_timestamped_line(
        "Finished running {stat_line} in {execution_time:0.2f}s."
        .format(stat_line=stat_line, execution_time=execution_time))


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

    if dbt.flags.NON_DESTRUCTIVE:
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
        "model_materialization": get_materialization(run_model_result.node),  # noqa
        "model_id": get_hash(run_model_result.node),
        "hashed_contents": get_hashed_contents(run_model_result.node),  # noqa
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

    def compile_node(self, node, flat_graph):
        compiler = dbt.compilation.Compiler(self.project)
        node = compiler.compile_node(node, flat_graph)
        return node

    def safe_compile_node(self, data):
        node = data['node']
        flat_graph = data['flat_graph']

        result = RunModelResult(node)
        profile = self.project.run_environment()
        adapter = get_adapter(profile)

        try:
            compiled_node = self.compile_node(node, flat_graph)
            result = RunModelResult(compiled_node)

        finally:
            adapter.release_connection(profile, node.get('name'))

        return result

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

        try:
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

        except dbt.exceptions.CompilationException as e:
            return RunModelResult(
                node,
                error=str(e),
                status='ERROR')

        except (RuntimeError,
                dbt.exceptions.ProgrammingException,
                psycopg2.ProgrammingError,
                psycopg2.InternalError) as e:

            prefix = "Error executing {}\n".format(node.get('build_path'))
            error = "{}{}".format(dbt.ui.printer.red(prefix), str(e).strip())

            status = "ERROR"
            logger.debug(error)
            if type(e) == psycopg2.InternalError and \
               ABORTED_TRANSACTION_STRING == e.diag.message_primary:
                return RunModelResult(
                    node,
                    error='{}\n'.format(ABORTED_TRANSACTION_STRING),
                    status="SKIP")

        except dbt.exceptions.InternalException as e:

            build_path = node.get('build_path')
            prefix = 'Internal error executing {}'.format(build_path)

            error = "{prefix}\n{error}\n\n{note}".format(
                         prefix=dbt.ui.printer.red(prefix),
                         error=str(e).strip(),
                         note=INTERNAL_ERROR_STRING)
            logger.debug(error)

            status = "ERROR"

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

        execution_time = time.time() - start_time

        result = RunModelResult(node,
                                error=error,
                                status=status,
                                execution_time=execution_time)

        if not is_ephemeral:
            print_result_line(result, schema_name, node_index, num_nodes)

        return result

    def as_flat_dep_list(self, linker, nodes_to_run):
        dependency_list = linker.as_dependency_list(
            nodes_to_run,
            ephemeral_only=True)

        concurrent_dependency_list = []
        for level in dependency_list:
            node_level = [linker.get_node(node) for node in level]
            concurrent_dependency_list.append(node_level)

        return concurrent_dependency_list

    def as_concurrent_dep_list(self, linker, nodes_to_run):
        dependency_list = linker.as_dependency_list(nodes_to_run)

        concurrent_dependency_list = []
        for level in dependency_list:
            node_level = [linker.get_node(node) for node in level]
            concurrent_dependency_list.append(node_level)

        return concurrent_dependency_list

    def run_hooks(self, profile, flat_graph, hook_type):
        adapter = get_adapter(profile)

        nodes = flat_graph.get('nodes', {}).values()
        start_hooks = get_nodes_by_tags(nodes, {hook_type}, NodeType.Operation)
        hooks = [self.compile_node(hook, flat_graph) for hook in start_hooks]

        master_connection = adapter.begin(profile)
        compiled_hooks = [hook['wrapped_sql'] for hook in hooks]
        adapter.execute_all(profile=profile, sqls=compiled_hooks)
        master_connection = adapter.commit(master_connection)

    def on_model_failure(self, linker, selected_nodes):
        def skip_dependent(node):
            dependent_nodes = linker.get_dependent_nodes(node.get('unique_id'))
            for node in dependent_nodes:
                if node in selected_nodes:
                    node_data = linker.get_node(node)
                    node_data['skip'] = True
                    linker.update_node_data(node, node_data)

        return skip_dependent

    def execute_nodes(self, flat_graph, node_dependency_list, on_failure,
                      should_run_hooks=False, should_execute=True):
        profile = self.project.run_environment()
        adapter = get_adapter(profile)
        master_connection = adapter.get_connection(profile)
        schema_name = adapter.get_default_schema(profile)

        flat_nodes = list(itertools.chain.from_iterable(
            node_dependency_list))

        if len(flat_nodes) == 0:
            logger.info("WARNING: Nothing to do. Try checking your model "
                        "configs and model specification args")
            return []

        num_threads = self.threads
        logger.info("Concurrency: {} threads (target='{}')".format(
            num_threads, self.project.get_target().get('name'))
        )

        master_connection = adapter.begin(profile)
        existing = adapter.query_for_existing(profile, schema_name)
        master_connection = adapter.commit(master_connection)

        node_id_to_index_map = {}
        i = 1

        for node in flat_nodes:
            if get_materialization(node) != 'ephemeral':
                node_id_to_index_map[node.get('unique_id')] = i
                i += 1

        num_nodes = len(node_id_to_index_map)

        pool = ThreadPool(num_threads)

        if should_execute:
            stat_line = dbt.ui.printer.get_counts(flat_nodes)
            full_line = "Running {}".format(stat_line)

            logger.info("")
            dbt.ui.printer.print_timestamped_line(full_line)
            dbt.ui.printer.print_timestamped_line("")

        start_time = time.time()

        if should_run_hooks:
            self.run_hooks(profile, flat_graph, dbt.utils.RunHookType.Start)

        def get_idx(node):
            return node_id_to_index_map.get(node.get('unique_id'))

        node_results = []

        for node_list in node_dependency_list:
            for i, node in enumerate([node for node in node_list
                                      if node.get('skip')]):
                node_name = node.get('name')
                dbt.ui.printer.print_skip_line(node, schema_name, node_name,
                                               get_idx(node), num_nodes)

                node_result = RunModelResult(node, skip=True)
                node_results.append(node_result)

            nodes_to_execute = [node for node in node_list
                                if not node.get('skip')]

            if should_execute:
                action = self.safe_execute_node
            else:
                action = self.safe_compile_node

            node_result = []
            try:
                args_list = []
                for node in nodes_to_execute:
                    args_list.append({
                        'node': node,
                        'flat_graph': flat_graph,
                        'existing': existing,
                        'schema_name': schema_name,
                        'node_index': get_idx(node),
                        'num_nodes': num_nodes
                    })

                for result in pool.imap_unordered(action, args_list):
                    node_results.append(result)

                    # propagate so that CTEs get injected properly
                    node_id = result.node.get('unique_id')
                    flat_graph['nodes'][node_id] = result.node

                    index = get_idx(result.node)
                    if should_execute:
                        track_model_run(index, num_nodes, result)

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

        if should_run_hooks:
            self.run_hooks(profile, flat_graph, dbt.utils.RunHookType.End)

        execution_time = time.time() - start_time

        if should_execute:
            print_results_line(node_results, execution_time)

        return node_results

    def get_ancestor_ephemeral_nodes(self, flat_graph, linked_graph,
                                     selected_nodes):
        node_names = {
            node: flat_graph['nodes'].get(node).get('name')
            for node in selected_nodes
            if node in flat_graph['nodes']
        }

        include_spec = [
            '+{}'.format(node_names[node])
            for node in selected_nodes if node in node_names
        ]

        all_ancestors = dbt.graph.selector.select_nodes(
            self.project,
            linked_graph,
            include_spec,
            [])

        res = []

        for ancestor in all_ancestors:
            if ancestor not in flat_graph['nodes']:
                continue
            ancestor_node = flat_graph['nodes'][ancestor]
            is_model = ancestor_node.get('resource_type') == NodeType.Model
            is_ephemeral = get_materialization(ancestor_node) == 'ephemeral'
            if is_model and is_ephemeral:
                res.append(ancestor)

        return set(res)

    def get_nodes_to_run(self, graph, include_spec, exclude_spec,
                         resource_types, tags):

        if include_spec is None:
            include_spec = ['*']

        if exclude_spec is None:
            exclude_spec = []

        to_run = [
            n for n in graph.nodes()
            if (graph.node.get(n).get('empty') is False and
                is_enabled(graph.node.get(n)))
        ]

        filtered_graph = graph.subgraph(to_run)
        selected_nodes = dbt.graph.selector.select_nodes(self.project,
                                                         filtered_graph,
                                                         include_spec,
                                                         exclude_spec)

        post_filter = [
            n for n in selected_nodes
            if ((graph.node.get(n).get('resource_type') in resource_types) and
                (len(tags) == 0 or
                 # does the node share any tags with the run?
                 bool(graph.node.get(n).get('tags') & tags)))
        ]

        return set(post_filter)

    def try_create_schema(self):
        profile = self.project.run_environment()
        adapter = get_adapter(profile)

        schema_name = adapter.get_default_schema(profile)
        model_name = None

        connection = adapter.begin(profile)
        schema_exists = adapter.check_schema_exists(profile, schema_name)
        adapter.commit(connection)

        if schema_exists:
            logger.debug('schema {} already exists -- '
                         'not creating'.format(schema_name))
            return

        try:
            connection = adapter.begin(profile)
            adapter.create_schema(profile, schema_name)
            adapter.commit(connection)

        except (dbt.exceptions.FailedToConnectException,
                psycopg2.OperationalError) as e:
            logger.info("ERROR: Could not connect to the target database. Try "
                        "`dbt debug` for more information.")
            logger.info(str(e))
            raise

    def run_types_from_graph(self, include_spec, exclude_spec,
                             resource_types, tags, should_run_hooks=False,
                             flatten_graph=False, should_execute=True):

        compiler = dbt.compilation.Compiler(self.project)
        compiler.initialize()
        (flat_graph, linker) = compiler.compile()

        selected_nodes = self.get_nodes_to_run(
            linker.graph,
            include_spec,
            exclude_spec,
            resource_types,
            tags)

        # automatically pull in ephemeral models required by selected nodes.
        ephemeral_models = self.get_ancestor_ephemeral_nodes(
            flat_graph,
            linker.graph,
            selected_nodes)

        selected_nodes = selected_nodes | ephemeral_models

        dependency_list = []

        if flatten_graph is False:
            dependency_list = self.as_concurrent_dep_list(linker,
                                                          selected_nodes)
        else:
            dependency_list = self.as_flat_dep_list(linker,
                                                    selected_nodes)

        profile = self.project.run_environment()
        adapter = get_adapter(profile)

        try:
            if should_execute:
                self.try_create_schema()

            on_failure = self.on_model_failure(linker, selected_nodes)

            results = self.execute_nodes(flat_graph, dependency_list,
                                         on_failure, should_run_hooks,
                                         should_execute)

        finally:
            adapter.cleanup_connections()

        return results

    # ------------------------------------

    def compile_models(self, include_spec, exclude_spec):
        resource_types = [
            NodeType.Model,
            NodeType.Test,
            NodeType.Archive,
            NodeType.Analysis,
            NodeType.Operation
        ]

        return self.run_types_from_graph(include_spec,
                                         exclude_spec,
                                         resource_types=resource_types,
                                         tags=set(),
                                         should_run_hooks=False,
                                         should_execute=False)

    def run_models(self, include_spec, exclude_spec):
        return self.run_types_from_graph(include_spec,
                                         exclude_spec,
                                         resource_types=[NodeType.Model],
                                         tags=set(),
                                         should_run_hooks=True)

    def run_tests(self, include_spec, exclude_spec, tags):
        return self.run_types_from_graph(include_spec,
                                         exclude_spec,
                                         resource_types=[NodeType.Test],
                                         tags=tags,
                                         flatten_graph=True)

    def run_archives(self, include_spec, exclude_spec):
        return self.run_types_from_graph(include_spec,
                                         exclude_spec,
                                         resource_types=[NodeType.Archive],
                                         tags=set(),
                                         flatten_graph=True)
