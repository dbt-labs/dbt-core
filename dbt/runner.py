
from __future__ import print_function

import psycopg2
import os
import sys
import logging
import time
import itertools
import re
import yaml
from datetime import datetime

from dbt.adapters.factory import get_adapter
from dbt.logger import GLOBAL_LOGGER as logger
import dbt.compilation
from dbt.linker import Linker
from dbt.source import Source
from dbt.utils import find_model_by_fqn, find_model_by_name, \
    dependency_projects
from dbt.compiled_model import make_compiled_model

import dbt.exceptions
import dbt.tracking
import dbt.schema
import dbt.graph.selector
import dbt.model

from multiprocessing.dummy import Pool as ThreadPool

ABORTED_TRANSACTION_STRING = ("current transaction is aborted, commands "
                              "ignored until end of transaction block")


def get_timestamp():
    return "{} |".format(time.strftime("%H:%M:%S"))


class RunModelResult(object):
    def __init__(self, model, error=None, skip=False, status=None,
                 execution_time=0):
        self.model = model
        self.error = error
        self.skip = skip
        self.status = status
        self.execution_time = execution_time

    @property
    def errored(self):
        return self.error is not None

    @property
    def skipped(self):
        return self.skip


class BaseRunner(object):
    def __init__(self, project):
        self.project = project

        self.profile = project.run_environment()
        self.adapter = get_adapter(self.profile)

    def pre_run_msg(self, model):
        raise NotImplementedError("not implemented")

    def skip_msg(self, model):
        return "SKIP relation {}.{}".format(
            self.adapter.get_default_schema(self.profile), model.name)

    def post_run_msg(self, result):
        raise NotImplementedError("not implemented")

    def pre_run_all_msg(self, models):
        raise NotImplementedError("not implemented")

    def post_run_all_msg(self, results):
        raise NotImplementedError("not implemented")

    def post_run_all(self, models, results, context):
        pass

    def pre_run_all(self, models, context):
        pass

    def status(self, result):
        raise NotImplementedError("not implemented")


class ModelRunner(BaseRunner):
    run_type = dbt.model.NodeType.Model

    def pre_run_msg(self, model):
        print_vars = {
            "schema": self.adapter.get_default_schema(self.profile),
            "model_name": model.name,
            "model_type": model.materialization,
            "info": "START"
        }

        output = ("START {model_type} model {schema}.{model_name} "
                  .format(**print_vars))
        return output

    def post_run_msg(self, result):
        model = result.model
        print_vars = {
            "schema": self.adapter.get_default_schema(self.profile),
            "model_name": model.name,
            "model_type": model.materialization,
            "info": "ERROR creating" if result.errored else "OK created"
        }

        output = ("{info} {model_type} model {schema}.{model_name} "
                  .format(**print_vars))
        return output

    def pre_run_all_msg(self, models):
        return "{} Running {} models".format(get_timestamp(), len(models))

    def post_run_all_msg(self, results):
        return ("{} Finished running {} models"
                .format(get_timestamp(), len(results)))

    def status(self, result):
        return result.status

    def is_non_destructive(self):
        if hasattr(self.project.args, 'non_destructive'):
            return self.project.args.non_destructive
        else:
            return False

    def execute(self, model):
        profile = self.project.run_environment()
        adapter = get_adapter(profile)

        if model.tmp_drop_type is not None:
            if model.materialization == 'table' and self.is_non_destructive():
                adapter.truncate(
                    profile=profile,
                    table=model.tmp_name,
                    model_name=model.name)
            else:
                adapter.drop(
                    profile=profile,
                    relation=model.tmp_name,
                    relation_type=model.tmp_drop_type,
                    model_name=model.name)

        status = adapter.execute_model(
            profile=profile,
            model=model)

        if model.final_drop_type is not None:
            if model.materialization == 'table' and self.is_non_destructive():
                # we just inserted into this recently truncated table...
                # do nothing here
                pass
            else:
                adapter.drop(
                    profile=profile,
                    relation=model.name,
                    relation_type=model.final_drop_type,
                    model_name=model.name)

        if model.should_rename(self.project.args):
            adapter.rename(
                profile=profile,
                from_name=model.tmp_name,
                to_name=model.name,
                model_name=model.name)

        adapter.commit(
            profile=profile)

        return status

    def __run_hooks(self, hooks, context, source):
        if type(hooks) not in (list, tuple):
            hooks = [hooks]

        target = self.project.get_target()

        ctx = {
            "target": target,
            "state": "start",
            "invocation_id": context['invocation_id'],
            "run_started_at": context['run_started_at']
        }

        compiled_hooks = [
            dbt.compilation.compile_string(hook, ctx) for hook in hooks
        ]

        profile = self.project.run_environment()
        adapter = get_adapter(profile)

        adapter.execute_all(
            profile=profile,
            queries=compiled_hooks,
            model_name=source)

        adapter.commit(profile)

    def pre_run_all(self, models, context):
        hooks = self.project.cfg.get('on-run-start', [])
        self.__run_hooks(hooks, context, 'on-run-start hooks')

    def post_run_all(self, models, results, context):
        hooks = self.project.cfg.get('on-run-end', [])
        self.__run_hooks(hooks, context, 'on-run-end hooks')


class TestRunner(ModelRunner):
    run_type = dbt.model.NodeType.Test

    test_data_type = dbt.model.TestNodeType.DataTest
    test_schema_type = dbt.model.TestNodeType.SchemaTest

    def pre_run_msg(self, model):
        if model.is_test_type(self.test_data_type):
            return "DATA TEST {name} ".format(name=model.name)
        else:
            return "SCHEMA TEST {name} ".format(name=model.name)

    def post_run_msg(self, result):
        model = result.model
        info = self.status(result)

        return "{info} {name} ".format(info=info, name=model.name)

    def pre_run_all_msg(self, models):
        return "{} Running {} tests".format(get_timestamp(), len(models))

    def post_run_all_msg(self, results):
        total = len(results)
        passed = len([result for result in results if not
                      result.errored and not result.skipped and
                      result.status == 0])
        failed = len([result for result in results if not
                      result.errored and not result.skipped and
                      result.status > 0])
        errored = len([result for result in results if result.errored])
        skipped = len([result for result in results if result.skipped])

        total_errors = failed + errored

        overview = ("PASS={passed} FAIL={total_errors} SKIP={skipped} "
                    "TOTAL={total}".format(
                        total=total,
                        passed=passed,
                        total_errors=total_errors,
                        skipped=skipped))

        if total_errors > 0:
            final = "Tests completed with errors"
        else:
            final = "All tests passed"

        return "\n{overview}\n{final}".format(overview=overview, final=final)

    def status(self, result):
        if result.errored:
            info = "ERROR"
        elif result.status > 0:
            info = 'FAIL {}'.format(result.status)
        elif result.status == 0:
            info = 'PASS'
        else:
            raise RuntimeError("unexpected status: {}".format(result.status))

        return info

    def execute(self, model):
        profile = self.project.run_environment()
        adapter = get_adapter(profile)

        _, cursor = adapter.execute_one(
            profile, model.compiled_contents, model.name)
        rows = cursor.fetchall()

        cursor.close()

        if len(rows) > 1:
            raise RuntimeError(
                "Bad test {name}: Returned {num_rows} rows instead of 1"
                .format(name=model.name, num_rows=len(rows)))

        row = rows[0]
        if len(row) > 1:
            raise RuntimeError(
                "Bad test {name}: Returned {num_cols} cols instead of 1"
                .format(name=model.name, num_cols=len(row)))

        return row[0]


class ArchiveRunner(BaseRunner):
    run_type = dbt.model.NodeType.Archive

    def pre_run_msg(self, model):
        print_vars = {
            "schema": self.adapter.get_default_schema(self.profile),
            "model_name": model.name,
        }

        output = ("START archive table {schema}.{model_name} "
                  .format(**print_vars))
        return output

    def post_run_msg(self, result):
        model = result.model
        print_vars = {
            "schema": self.adapter.get_default_schema(self.profile),
            "model_name": model.name,
            "info": "ERROR archiving" if result.errored else "OK created"
        }

        output = "{info} table {schema}.{model_name} ".format(**print_vars)
        return output

    def pre_run_all_msg(self, models):
        return "Archiving {} tables".format(len(models))

    def post_run_all_msg(self, results):
        return ("{} Finished archiving {} tables"
                .format(get_timestamp(), len(results)))

    def status(self, result):
        return result.status

    def execute(self, model):
        profile = self.project.run_environment()
        adapter = get_adapter(profile)

        status = adapter.execute_model(
            profile=profile,
            model=model)

        return status


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

        adapter = get_adapter(profile)
        schema_name = adapter.get_default_schema(profile)

        self.existing_models = adapter.query_for_existing(
            profile,
            schema_name
        )

        def call_get_columns_in_table(schema_name, table_name):
            return adapter.get_columns_in_table(
                profile, schema_name, table_name)

        def call_get_missing_columns(from_schema, from_table,
                                     to_schema, to_table):
            return adapter.get_missing_columns(
                profile, from_schema, from_table,
                to_schema, to_table)

        def call_table_exists(schema, table):
            return adapter.table_exists(
                profile, schema, table)

        self.context = {
            "run_started_at": datetime.now(),
            "invocation_id": dbt.tracking.active_user.invocation_id,
            "get_columns_in_table": call_get_columns_in_table,
            "get_missing_columns": call_get_missing_columns,
            "already_exists": call_table_exists,
        }

    def deserialize_graph(self):
        logger.info("Loading dependency graph file")

        linker = Linker()
        base_target_path = self.project['target-path']
        graph_file = os.path.join(
            base_target_path,
            dbt.compilation.graph_file_name
        )
        linker.read_graph(graph_file)

        return linker

    def execute_model(self, runner, model):
        logger.debug("executing model %s", model)

        result = runner.execute(model)
        return result

    def safe_execute_model(self, data):
        runner, model = data['runner'], data['model']

        start_time = time.time()

        error = None
        try:
            status = self.execute_model(runner, model)
        except (RuntimeError,
                dbt.exceptions.ProgrammingException,
                psycopg2.ProgrammingError,
                psycopg2.InternalError) as e:
            error = "Error executing {filepath}\n{error}".format(
                filepath=model['build_path'], error=str(e).strip())
            status = "ERROR"
            logger.debug(error)
            if type(e) == psycopg2.InternalError and \
               ABORTED_TRANSACTION_STRING == e.diag.message_primary:
                return RunModelResult(
                    model, error=ABORTED_TRANSACTION_STRING, status="SKIP")
        except Exception as e:
            error = ("Unhandled error while executing {filepath}\n{error}"
                     .format(
                         filepath=model['build_path'], error=str(e).strip()))
            logger.debug(error)
            raise e

        execution_time = time.time() - start_time

        return RunModelResult(model,
                              error=error,
                              status=status,
                              execution_time=execution_time)

    def as_concurrent_dep_list(self, linker, models_to_run):
        # linker.as_dependency_list operates on nodes, but this method operates
        # on compiled models. Use a dict to translate between the two
        node_model_map = {m.fqn: m for m in models_to_run}
        dependency_list = linker.as_dependency_list(node_model_map.keys())

        model_dependency_list = []
        for node_level in dependency_list:
            model_level = [node_model_map[n] for n in node_level]
            model_dependency_list.append(model_level)

        return model_dependency_list

    def on_model_failure(self, linker, models, selected_nodes):
        def skip_dependent(model):
            dependent_nodes = linker.get_dependent_nodes(model.fqn)
            for node in dependent_nodes:
                if node in selected_nodes:
                    try:
                        model_to_skip = find_model_by_fqn(models, node)
                        model_to_skip.do_skip()
                    except RuntimeError as e:
                        pass
        return skip_dependent

    def print_fancy_output_line(self, message, status, index, total,
                                execution_time=None):
        prefix = "{timestamp} {index} of {total} {message}".format(
            timestamp=get_timestamp(),
            index=index,
            total=total,
            message=message)
        justified = prefix.ljust(80, ".")

        if execution_time is None:
            status_time = ""
        else:
            status_time = " in {execution_time:0.2f}s".format(
                execution_time=execution_time)

        output = "{justified} [{status}{status_time}]".format(
            justified=justified, status=status, status_time=status_time)
        logger.info(output)

    def execute_models(self, runner, model_dependency_list, on_failure):
        flat_models = list(itertools.chain.from_iterable(
            model_dependency_list))

        num_models = len(flat_models)
        if num_models == 0:
            logger.info("WARNING: Nothing to do. Try checking your model "
                        "configs and running `dbt compile`".format(
                            self.target_path))
            return []

        num_threads = self.threads
        logger.info("Concurrency: {} threads (target='{}')".format(
            num_threads, self.project.get_target().get('name'))
        )
        logger.info("Running!")

        pool = ThreadPool(num_threads)

        logger.info("")
        logger.info(runner.pre_run_all_msg(flat_models))
        runner.pre_run_all(flat_models, self.context)

        fqn_to_id_map = {model.fqn: i + 1 for (i, model)
                         in enumerate(flat_models)}

        def get_idx(model):
            return fqn_to_id_map[model.fqn]

        model_results = []
        for model_list in model_dependency_list:
            for i, model in enumerate([model for model in model_list
                                       if model.should_skip()]):
                msg = runner.skip_msg(model)
                self.print_fancy_output_line(
                    msg, 'SKIP', get_idx(model), num_models)
                model_result = RunModelResult(model, skip=True)
                model_results.append(model_result)

            models_to_execute = [model for model in model_list
                                 if not model.should_skip()]

            threads = self.threads
            num_models_this_batch = len(models_to_execute)
            model_index = 0

            def on_complete(run_model_results):
                for run_model_result in run_model_results:
                    model_results.append(run_model_result)

                    msg = runner.post_run_msg(run_model_result)
                    status = runner.status(run_model_result)
                    index = get_idx(run_model_result.model)
                    self.print_fancy_output_line(
                        msg,
                        status,
                        index,
                        num_models,
                        run_model_result.execution_time
                    )

                    invocation_id = dbt.tracking.active_user.invocation_id
                    dbt.tracking.track_model_run({
                        "invocation_id": invocation_id,
                        "index": index,
                        "total": num_models,
                        "execution_time": run_model_result.execution_time,
                        "run_status": run_model_result.status,
                        "run_skipped": run_model_result.skip,
                        "run_error": run_model_result.error,
                        "model_materialization": run_model_result.model['materialized'],  # noqa
                        "model_id": run_model_result.model.hashed_name(),
                        "hashed_contents": run_model_result.model.hashed_contents(),  # noqa
                    })

                    if run_model_result.errored:
                        on_failure(run_model_result.model)
                        logger.info(run_model_result.error)

            while model_index < num_models_this_batch:
                local_models = []
                for i in range(
                        model_index,
                        min(model_index + threads, num_models_this_batch)):
                    model = models_to_execute[i]
                    local_models.append(model)
                    msg = runner.pre_run_msg(model)
                    self.print_fancy_output_line(
                        msg, 'RUN', get_idx(model), num_models
                    )

                wrapped_models_to_execute = [
                    {"runner": runner, "model": model}
                    for model in local_models
                ]
                map_result = pool.map_async(
                    self.safe_execute_model,
                    wrapped_models_to_execute,
                    callback=on_complete
                )
                map_result.wait()
                run_model_results = map_result.get()

                model_index += threads

        pool.close()
        pool.join()

        logger.info("")
        logger.info(runner.post_run_all_msg(model_results))
        runner.post_run_all(flat_models, model_results, self.context)

        return model_results

    def get_nodes_to_run(self, graph, include_spec, exclude_spec, model_type):
        if include_spec is None:
            include_spec = ['*']

        if exclude_spec is None:
            exclude_spec = []

        model_nodes = [
            n for n in graph.nodes()
            if graph.node[n]['dbt_run_type'] == model_type
        ]

        model_only_graph = graph.subgraph(model_nodes)
        selected_nodes = dbt.graph.selector.select_nodes(self.project,
                                                         model_only_graph,
                                                         include_spec,
                                                         exclude_spec)
        return selected_nodes

    def get_compiled_models(self, linker, nodes, node_type):
        compiled_models = []
        for fqn in nodes:
            compiled_model = make_compiled_model(fqn, linker.get_node(fqn))

            if not compiled_model.is_type(node_type):
                continue

            if not compiled_model.should_execute(self.args,
                                                 self.existing_models):
                continue

            context = self.context.copy()
            context.update(compiled_model.context())

            profile = self.project.run_environment()
            compiled_model.compile(context, profile, self.existing_models)
            compiled_models.append(compiled_model)

        return compiled_models

    def try_create_schema(self):
        profile = self.project.run_environment()
        adapter = get_adapter(profile)

        try:
            schema_name = adapter.get_default_schema(profile)

            adapter.create_schema(profile, schema_name)
        except (dbt.exceptions.FailedToConnectException,
                psycopg2.OperationalError) as e:
            logger.info("ERROR: Could not connect to the target database. Try "
                        "`dbt debug` for more information.")
            logger.info(str(e))
            raise

    def run_models_from_graph(self, include_spec, exclude_spec):
        runner = ModelRunner(self.project)
        linker = self.deserialize_graph()

        selected_nodes = self.get_nodes_to_run(
            linker.graph,
            include_spec,
            exclude_spec,
            dbt.model.NodeType.Model)

        compiled_models = self.get_compiled_models(
            linker,
            selected_nodes,
            runner.run_type)

        self.try_create_schema()

        model_dependency_list = self.as_concurrent_dep_list(
            linker,
            compiled_models
        )

        on_failure = self.on_model_failure(linker, compiled_models,
                                           selected_nodes)
        results = self.execute_models(
            runner, model_dependency_list, on_failure
        )

        return results

    def run_tests_from_graph(self, include_spec, exclude_spec,
                             test_schemas, test_data):

        runner = TestRunner(self.project)
        linker = self.deserialize_graph()

        selected_model_nodes = self.get_nodes_to_run(
            linker.graph,
            include_spec,
            exclude_spec,
            dbt.model.NodeType.Model)

        # just throw everything in this set, then pick out tests later
        nodes_and_neighbors = set()
        for model_node in selected_model_nodes:
            nodes_and_neighbors.add(model_node)
            neighbors = linker.graph.neighbors(model_node)
            for neighbor in neighbors:
                nodes_and_neighbors.add(neighbor)

        compiled_models = self.get_compiled_models(
            linker,
            nodes_and_neighbors,
            runner.run_type)

        selected_nodes = set(cm.fqn for cm in compiled_models)

        self.try_create_schema()

        all_tests = []
        if test_schemas:
            all_tests.extend([cm for cm in compiled_models
                             if cm.is_test_type(runner.test_schema_type)])

        if test_data:
            all_tests.extend([cm for cm in compiled_models
                              if cm.is_test_type(runner.test_data_type)])

        dep_list = [all_tests]

        on_failure = self.on_model_failure(linker, all_tests, selected_nodes)
        results = self.execute_models(runner, dep_list, on_failure)

        return results

    def run_archives_from_graph(self):
        runner = ArchiveRunner(self.project)
        linker = self.deserialize_graph()

        selected_nodes = self.get_nodes_to_run(
            linker.graph,
            None,
            None,
            dbt.model.NodeType.Archive)

        compiled_models = self.get_compiled_models(
            linker,
            selected_nodes,
            runner.run_type)

        self.try_create_schema()

        model_dependency_list = self.as_concurrent_dep_list(
            linker,
            compiled_models
        )

        on_failure = self.on_model_failure(linker, compiled_models,
                                           selected_nodes)
        results = self.execute_models(
            runner, model_dependency_list, on_failure
        )

        return results

    # ------------------------------------

    def run_tests(self, include_spec, exclude_spec,
                  test_schemas=False, test_data=False):
        return self.run_tests_from_graph(include_spec, exclude_spec,
                                         test_schemas, test_data)

    def run_models(self, include_spec, exclude_spec):
        return self.run_models_from_graph(include_spec, exclude_spec)

    def run_archives(self):
        return self.run_archives_from_graph()
