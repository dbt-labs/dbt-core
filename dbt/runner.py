
from __future__ import print_function

import psycopg2
import os, sys
import logging
import time

from dbt.compilation import Linker, Compiler
from dbt.templates import BaseCreateTemplate
from dbt.targets import RedshiftTarget
from dbt.source import Source
from dbt.utils import find_model_by_name

from multiprocessing.dummy import Pool as ThreadPool

SCHEMA_PERMISSION_DENIED_MESSAGE = """The user '{user}' does not have sufficient permissions to create the schema '{schema}'.
Either create the schema  manually, or adjust the permissions of the '{user}' user."""

RELATION_PERMISSION_DENIED_MESSAGE = """The user '{user}' does not have sufficient permissions to create the model '{model}'  in the schema '{schema}'.
Please adjust the permissions of the '{user}' user on the '{schema}' schema.
With a superuser account, execute the following commands, then re-run dbt.

grant usage, create on schema "{schema}" to "{user}";
grant select on all tables in schema "{schema}" to "{user}";"""

RELATION_NOT_OWNER_MESSAGE = """The user '{user}' does not have sufficient permissions to drop the model '{model}' in the schema '{schema}'.
This is likely because the relation was created by a different user. Either delete the model "{schema}"."{model}" manually,
or adjust the permissions of the '{user}' user in the '{schema}' schema."""

class RunModelResult(object):
    def __init__(self, model, error=None, skip=False, status=None):
        self.model = model
        self.error = error
        self.skip  = skip
        self.status = status

    @property
    def errored(self):
        return self.error is not None

    @property
    def skipped(self):
        return self.skip

class Runner:
    def __init__(self, project, target_path, run_mode):
        self.logger = logging.getLogger(__name__)
        self.project = project
        self.target_path = target_path
        self.run_mode = run_mode

    def get_compiled_models(self):
        return Source(self.project).get_compiled(self.target_path, self.run_mode)

    def get_target(self):
        target_cfg = self.project.run_environment()
        return RedshiftTarget(target_cfg)

    def deserialize_graph(self):
        linker = Linker()
        base_target_path = self.project['target-path']
        filename = 'graph-{}.yml'.format(self.run_mode)
        graph_file = os.path.join(base_target_path, filename)
        linker.read_graph(graph_file)

        return linker

    def create_schema(self, schema_name):
        target = self.get_target()
        with target.get_handle() as handle:
            with handle.cursor() as cursor:
                cursor.execute('create schema if not exists "{}"'.format(schema_name))

    def get_schemas(self):
        target = self.get_target()
        existing = []
        with target.get_handle() as handle:
            with handle.cursor() as cursor:
                cursor.execute('select nspname from pg_catalog.pg_namespace')

                existing = [name for (name,) in cursor.fetchall()]
        return existing

    def create_schema_or_exit(self, schema_name):

        target_cfg = self.project.run_environment()
        user = target_cfg['user']

        try:
            self.create_schema(schema_name)
        except psycopg2.ProgrammingError as e:
            if "permission denied for" in e.diag.message_primary:
                raise RuntimeError(SCHEMA_PERMISSION_DENIED_MESSAGE.format(schema=schema_name, user=user))
            else:
                raise e

    def query_for_existing(self, target, schema):
        sql = """
            select tablename as name, 'table' as type from pg_tables where schemaname = '{schema}'
                union all
            select viewname as name, 'view' as type from pg_views where schemaname = '{schema}' """.format(schema=schema)


        with target.get_handle() as handle:
            with handle.cursor() as cursor:
                cursor.execute(sql)
                existing = [(name, relation_type) for (name, relation_type) in cursor.fetchall()]

        return dict(existing)

    def get_drop_statement(self, schema, relation, relation_type):
        return 'drop {relation_type} if exists "{schema}"."{relation}" cascade'.format(schema=schema, relation_type=relation_type, relation=relation)

    def drop(self, target, model, relation, relation_type):
        sql = self.get_drop_statement(target.schema, relation, relation_type)
        self.logger.info("dropping %s %s.%s", relation_type, target.schema, relation)
        self.execute_and_handle_permissions(target, sql, model, relation)
        self.logger.info("dropped %s %s.%s", relation_type, target.schema, relation)

    def __do_execute(self, target, sql, model):
        with target.get_handle() as handle:
            with handle.cursor() as cursor:
                try:
                    self.logger.debug("SQL: %s", sql)
                    pre = time.time()
                    cursor.execute(sql)
                    post = time.time()
                    self.logger.debug("SQL status: %s in %d seconds", cursor.statusmessage, post-pre)
                    return cursor.statusmessage
                except Exception as e:
                    e.model = model
                    self.logger.exception("Error running SQL: %s", sql)
                    raise e

    def drop_models(self, models):
        target = self.get_target()

        existing = self.query_for_existing(target, target.schema);
        for model in models:
            model_name = model.fqn[-1]
            self.drop(target, model, model.name, existing[model_name])

    def get_model_by_fqn(self, models, fqn):
        for model in models:
            if tuple(model.fqn) == tuple(fqn):
                return model
        raise RuntimeError("Couldn't find a compiled model with fqn: '{}'".format(fqn))

    def execute_wrapped_model(self, data):
        target = data['target']
        model = data['model']
        tmp_drop_type = data['tmp_drop_type']
        final_drop_type = data['final_drop_type']

        error = None
        try:
            status = self.execute_model(target, model, tmp_drop_type, final_drop_type)
        except (RuntimeError, psycopg2.ProgrammingError) as e:
            error = "Error executing {filepath}\n{error}".format(filepath=model.filepath, error=str(e).strip())
            status = "ERROR"

        return RunModelResult(model, error=error, status=status)

    def execute_and_handle_permissions(self, target, query, model, model_name):
        try:
            return self.__do_execute(target, query, model)
        except psycopg2.ProgrammingError as e:
            error_data = {"model": model_name, "schema": target.schema, "user": target.user}
            if 'must be owner of relation' in e.diag.message_primary:
                raise RuntimeError(RELATION_NOT_OWNER_MESSAGE.format(**error_data))
            elif "permission denied for" in e.diag.message_primary:
                raise RuntimeError(RELATION_PERMISSION_DENIED_MESSAGE.format(**error_data))
            else:
                raise e

    def rename(self, target, model):
        rename_query = model.rename_query(target.schema)
        self.logger.info("renaming model %s.%s --> %s.%s", target.schema, model.tmp_name(), target.schema, model.name)
        self.execute_and_handle_permissions(target, rename_query, model, model.name)
        self.logger.info("renamed model %s.%s --> %s.%s", target.schema, model.tmp_name(), target.schema, model.name)

    def execute_model(self, target, model, tmp_drop_type, final_drop_type):
        self.logger.info("executing model %s", model)

        if tmp_drop_type is not None:
            self.drop(target, model, model.tmp_name(), tmp_drop_type)

        status = self.execute_and_handle_permissions(target, model.contents, model, model.tmp_name())

        if final_drop_type is not None:
            self.drop(target, model, model.name, final_drop_type)

        if not model.is_incremental:
            self.rename(target, model)

        return status

    def execute_models(self, linker, models, limit_to=None):
        target = self.get_target()

        dependency_list = linker.as_dependency_list(limit_to)
        existing = self.query_for_existing(target, target.schema);

        def wrap_fqn(target, models, existing, fqn):
            model = self.get_model_by_fqn(models, fqn)

            if model.is_incremental:
                tmp_drop_type = None
                final_drop_type = None
            else:
                tmp_drop_type = existing.get(model.tmp_name(), None) 
                final_drop_type = existing.get(model.name, None)
            return {"model" : model, "target": target, "tmp_drop_type": tmp_drop_type, 'final_drop_type': final_drop_type}

        # we can only pass one arg to the self.execute_model method below. Pass a dict w/ all the data we need
        model_dependency_list = []
        for node_list in dependency_list:
            level = []
            for fqn in node_list:
                node = linker.get_node(fqn)
                if node.get('materialized') == 'ephemeral':
                    continue
                else:
                    wrapped = wrap_fqn(target, models, existing, fqn)
                    level.append(wrapped)
            model_dependency_list.append(level)

        num_models = sum([len(node_list) for node_list in model_dependency_list])

        if num_models == 0:
            print("WARNING: No models to run in '{}'. Try checking your model configs and running `dbt compile`".format(self.target_path))
            return []

        num_threads = target.threads
        print("Concurrency: {} threads (target='{}')".format(num_threads, self.project['run-target']))
        print("Running!")

        pool = ThreadPool(num_threads)

        failed_models = set()

        model_results = []
        for model_list in model_dependency_list:
            failed_nodes = [tuple(model.fqn) for model in failed_models]

            models_to_execute = [data for data in model_list if not linker.is_child_of(failed_nodes, tuple(data['model'].fqn))]
            models_to_skip = [data for data in model_list if linker.is_child_of(failed_nodes, tuple(data['model'].fqn))]

            for i, data in enumerate(models_to_skip):
                model = data['model']
                model_result = RunModelResult(model, skip=True)
                model_results.append(model_result)
                print("{} of {} -- SKIP relation {}.{} because parent failed".format(len(model_results), num_models, target.schema, model_result.model.name))

            for i, model in enumerate(models_to_execute):
                print_vars = {
                    "progress": 1 + i + len(model_results),
                    "total" : num_models,
                    "schema": target.schema,
                    "model_name": model['model'].name,
                    "model_type": model['model'].materialization,
                    "info": "START"
                }

                output = "{progress} of {total} -- {info} {model_type} model {schema}.{model_name} ".format(**print_vars)
                print("{} [Running]".format(output.ljust(80, ".")))

            run_model_results = pool.map(self.execute_wrapped_model, models_to_execute)

            for run_model_result in run_model_results:
                model_results.append(run_model_result)

                print_vars = {
                    "progress": len(model_results),
                    "total" : num_models,
                    "schema": target.schema,
                    "model_name": run_model_result.model.name,
                    "model_type": run_model_result.model.materialization,
                    "info": "ERROR creating" if run_model_result.errored else "OK created"
                }

                output = "{progress} of {total} -- {info} {model_type} model {schema}.{model_name} ".format(**print_vars)
                print("{} [{}]".format(output.ljust(80, "."), run_model_result.status))

                if run_model_result.errored:
                    failed_models.add(run_model_result.model)
                    print(run_model_result.error)

        pool.close()
        pool.join()

        return model_results

    def run(self, specified_models=None):
        linker = self.deserialize_graph()
        compiled_models = self.get_compiled_models()

        limit_to = None
        if specified_models is not None:
            limit_to = []
            for model_name in specified_models:
                try:
                    model = find_model_by_name(compiled_models, model_name)
                    limit_to.append(tuple(model.fqn))
                except RuntimeError as e:
                    print("ERROR: {}".format(str(e)))
                    print("Exiting")
                    return[]

        target_cfg = self.project.run_environment()
        schema_name = target_cfg['schema']

        try:
            schemas = self.get_schemas()

            if schema_name not in schemas:
                self.create_schema_or_exit(schema_name)

            return self.execute_models(linker, compiled_models, limit_to)
        except psycopg2.OperationalError as e:
            print("ERROR: Could not connect to the target database. Try `dbt debug` for more information")
            print(str(e))
            sys.exit(1)
