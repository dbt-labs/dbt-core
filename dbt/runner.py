
from __future__ import print_function

import psycopg2
import os, sys
import logging
import time
import itertools
import re
import yaml
from datetime import datetime

from dbt.compilation import Compiler
from dbt.linker import Linker
from dbt.templates import BaseCreateTemplate
import dbt.targets
from dbt.source import Source
from dbt.utils import find_model_by_fqn, find_model_by_name, dependency_projects
from dbt.compiled_model import make_compiled_model
import dbt.tracking
import dbt.schema

from multiprocessing.dummy import Pool as ThreadPool

ABORTED_TRANSACTION_STRING = "current transaction is aborted, commands ignored until end of transaction block"

class RunModelResult(object):
    def __init__(self, model, error=None, skip=False, status=None, execution_time=0):
        self.model = model
        self.error = error
        self.skip  = skip
        self.status = status
        self.execution_time = execution_time

    @property
    def errored(self):
        return self.error is not None

    @property
    def skipped(self):
        return self.skip

class BaseRunner(object):
    def pre_run_msg(self, model):
        raise NotImplementedError("not implemented")

    def skip_msg(self, model):
        return "SKIP relation {}.{}".format(model.target.schema, model.name)

    def post_run_msg(self, result):
        raise NotImplementedError("not implemented")

    def pre_run_all_msg(self, models):
        raise NotImplementedError("not implemented")

    def post_run_all_msg(self, results):
        raise NotImplementedError("not implemented")

    def post_run_all(self, schema, results):
        pass

    def pre_run_all(self, models):
        pass

    def status(self, result):
        raise NotImplementedError("not implemented")

    def execute_contents(self, schema, target, model):
        parts = re.split(r'-- (DBT_OPERATION .*)', model.compiled_contents)
        handle = None

        status = 'None'
        for i, part in enumerate(parts):
            matches = re.match(r'^DBT_OPERATION ({.*})$', part)
            if matches is not None:
                instruction_string = matches.groups()[0]
                instruction = yaml.safe_load(instruction_string)
                function = instruction['function']
                kwargs = instruction['args']

                func_map = {
                    'expand_column_types_if_needed': lambda kwargs: schema.expand_column_types_if_needed(**kwargs),
                }

                func_map[function](kwargs)
            else:
                handle, status = schema.execute_without_auto_commit(part, handle)

        handle.commit()
        return status

class ModelRunner(BaseRunner):
    run_type = 'run'
    def pre_run_msg(self, model):
        print_vars = {
            "schema": model.target.schema,
            "model_name": model.name,
            "model_type": model.materialization,
            "info": "START"
        }

        output = "START {model_type} model {schema}.{model_name} ".format(**print_vars)
        return output

    def post_run_msg(self, result):
        model = result.model
        print_vars = {
            "schema": model.target.schema,
            "model_name": model.name,
            "model_type": model.materialization,
            "info": "ERROR creating" if result.errored else "OK created"
        }

        output = "{info} {model_type} model {schema}.{model_name} ".format(**print_vars)
        return output

    def pre_run_all_msg(self, models):
        return "Running {} models".format(len(models))

    def post_run_all_msg(self, results):
        return "Finished running {} models".format(len(results))

    def status(self, result):
        return result.status

    def execute(self, schema, target, model):
        if model.tmp_drop_type is not None:
            schema.drop(target.schema, model.tmp_drop_type, model.tmp_name)

        status = self.execute_contents(schema, target, model)

        if model.final_drop_type is not None:
            schema.drop(target.schema, model.final_drop_type, model.name)

        if model.should_rename():
            schema.rename(target.schema, model.tmp_name, model.name)

        return status

class DryRunner(ModelRunner):
    run_type = 'dry-run'

    def pre_run_msg(self, model):
        output = "DRY-RUN model {schema}.{model_name} ".format(schema=model.target.schema, model_name=model.name)
        return output

    def post_run_msg(self, result):
        model = result.model
        output = "DONE model {schema}.{model_name} ".format(schema=model.target.schema, model_name=model.name)
        return output

    def pre_run_all_msg(self, models):
        return "Dry-running {} models".format(len(models))

    def post_run_all_msg(self, results):
        return "Finished dry-running {} models".format(len(results))

    def post_run_all(self, schema, results):
        count_dropped = 0
        for result in results:
            if result.errored or result.skipped:
                continue
            model = result.model
            schema_name = model.target.schema

            relation_type = 'table' if model.materialization == 'incremental' else 'view'
            schema.drop(schema_name, relation_type, model.name)
            count_dropped += 1
        print("Dropped {} dry-run models".format(count_dropped))

class TestRunner(ModelRunner):
    run_type = 'test'

    def pre_run_msg(self, model):
        return "TEST {name} ".format(name=model.name)

    def post_run_msg(self, result):
        model = result.model
        info = self.status(result)

        return "{info} {name} ".format(info=info, name=model.name)

    def pre_run_all_msg(self, models):
        return "Running {} tests".format(len(models))

    def post_run_all_msg(self, results):
        total = len(results)
        passed  = len([result for result in results if not result.errored and not result.skipped and result.status == 0])
        failed  = len([result for result in results if not result.errored and not result.skipped and result.status > 0])
        errored = len([result for result in results if result.errored])
        skipped = len([result for result in results if result.skipped])

        total_errors = failed + errored

        overview = "PASS={passed} FAIL={total_errors} SKIP={skipped} TOTAL={total}".format(total=total, passed=passed, total_errors=total_errors, skipped=skipped)

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

    def execute(self, schema, target, model):
        rows = schema.execute_and_fetch(model.compiled_contents)
        if len(rows) > 1:
            raise RuntimeError("Bad test {name}: Returned {num_rows} rows instead of 1".format(name=model.name, num_rows=len(rows)))

        row = rows[0]
        if len(row) > 1:
            raise RuntimeError("Bad test {name}: Returned {num_cols} cols instead of 1".format(name=model.name, num_cols=len(row)))

        return row[0]

class ArchiveRunner(BaseRunner):
    run_type = 'archive'

    def pre_run_msg(self, model):
        print_vars = {
            "schema": model.target.schema,
            "model_name": model.name,
        }

        output = "START archive table {schema}.{model_name} ".format(**print_vars)
        return output

    def post_run_msg(self, result):
        model = result.model
        print_vars = {
            "schema": model.target.schema,
            "model_name": model.name,
            "info": "ERROR archiving" if result.errored else "OK created"
        }

        output = "{info} table {schema}.{model_name} ".format(**print_vars)
        return output

    def pre_run_all_msg(self, models):
        return "Archiving {} tables".format(len(models))

    def post_run_all_msg(self, results):
        return "Finished archiving {} tables".format(len(results))

    def status(self, result):
        return result.status

    def execute(self, schema, target, model):
        status = self.execute_contents(schema, target, model)
        return status

class RunManager(object):
    def __init__(self, project, target_path, graph_type, threads):
        self.logger = logging.getLogger(__name__)
        self.project = project
        self.target_path = target_path
        self.graph_type = graph_type

        self.target = dbt.targets.get_target(self.project.run_environment(), threads)

        if self.target.should_open_tunnel():
            print("Opening ssh tunnel to host {}... ".format(self.target.ssh_host), end="")
            sys.stdout.flush()
            self.target.open_tunnel_if_needed()
            print("Connected")


        self.schema = dbt.schema.Schema(self.project, self.target)

        self.context = {
            "run_started_at": datetime.now(),
            "invocation_id" : dbt.tracking.invocation_id,
            "get_columns_in_table"   : self.schema.get_columns_in_table,
            "get_missing_columns"   : self.schema.get_missing_columns,
            "already_exists"   : self.schema.table_exists,
        }


    def deserialize_graph(self):
        linker = Linker()
        base_target_path = self.project['target-path']
        filename = 'graph-{}.yml'.format(self.graph_type)
        graph_file = os.path.join(base_target_path, filename)
        linker.read_graph(graph_file)

        return linker

    def execute_model(self, runner, model):
        self.logger.info("executing model %s", model)

        result = runner.execute(self.schema, self.target, model)
        return result

    def safe_execute_model(self, data):
        runner, model = data['runner'], data['model']

        start_time = time.time()

        error = None
        try:
            status = self.execute_model(runner, model)
        except (RuntimeError, psycopg2.ProgrammingError, psycopg2.InternalError) as e:
            error = "Error executing {filepath}\n{error}".format(filepath=model['build_path'], error=str(e).strip())
            status = "ERROR"
            self.logger.exception(error)
            if type(e) == psycopg2.InternalError and ABORTED_TRANSACTION_STRING == e.diag.message_primary:
                return RunModelResult(model, error=ABORTED_TRANSACTION_STRING, status="SKIP")
        except Exception as e:
            error = "Unhandled error while executing {filepath}\n{error}".format(filepath=model['build_path'], error=str(e).strip())
            self.logger.exception(error)
            raise e

        execution_time = time.time() - start_time

        return RunModelResult(model, error=error, status=status, execution_time=execution_time)

    def as_concurrent_dep_list(self, linker, models, existing, target, limit_to):
        model_dependency_list = []
        dependency_list = linker.as_dependency_list(limit_to)
        for node_list in dependency_list:
            level = []
            for fqn in node_list:
                try:
                    model = find_model_by_fqn(models, fqn)
                except RuntimeError as e:
                    continue
                if model.should_execute():
                    model.prepare(existing, target)
                    level.append(model)
            model_dependency_list.append(level)
        return model_dependency_list

    def on_model_failure(self, linker, models):
        def skip_dependent(model):
            dependent_nodes = linker.get_dependent_nodes(model.fqn)
            for node in dependent_nodes:
                model_to_skip = find_model_by_fqn(models, node)
                model_to_skip.do_skip()
        return skip_dependent

    def print_fancy_output_line(self, message, status, index, total, execution_time=None):
        prefix = "{index} of {total} {message}".format(index=index, total=total, message=message)
        justified = prefix.ljust(80, ".")

        if execution_time is None:
            status_time = ""
        else:
            status_time = " in {execution_time:0.2f}s".format(execution_time=execution_time) 

        output = "{justified} [{status}{status_time}]".format(justified=justified, status=status, status_time=status_time)
        print(output)

    def execute_models(self, runner, model_dependency_list, on_failure):
        flat_models = list(itertools.chain.from_iterable(model_dependency_list))

        num_models = len(flat_models)
        if num_models == 0:
            print("WARNING: Nothing to do. Try checking your model configs and running `dbt compile`".format(self.target_path))
            return []

        num_threads = self.target.threads
        print("Concurrency: {} threads (target='{}')".format(num_threads, self.project['run-target']))
        print("Running!")

        pool = ThreadPool(num_threads)

        print()
        print(runner.pre_run_all_msg(flat_models))
        runner.pre_run_all(flat_models)

        fqn_to_id_map = {model.fqn: i + 1 for (i, model) in enumerate(flat_models)}

        def get_idx(model):
            return fqn_to_id_map[model.fqn]

        model_results = []
        for model_list in model_dependency_list:
            for i, model in enumerate([model for model in model_list if model.should_skip()]):
                msg = runner.skip_msg(model)
                self.print_fancy_output_line(msg, 'SKIP', get_idx(model), num_models)
                model_result = RunModelResult(model, skip=True)
                model_results.append(model_result)

            models_to_execute = [model for model in model_list if not model.should_skip()]

            threads = self.target.threads
            num_models_this_batch = len(models_to_execute)
            model_index = 0

            def on_complete(run_model_results):
                for run_model_result in run_model_results:
                    model_results.append(run_model_result)

                    msg = runner.post_run_msg(run_model_result)
                    status = runner.status(run_model_result)
                    index = get_idx(run_model_result.model)
                    self.print_fancy_output_line(msg, status, index, num_models, run_model_result.execution_time)

                    dbt.tracking.track_model_run({
                        "invocation_id": dbt.tracking.invocation_id,
                        "index": index,
                        "total": num_models,
                        "execution_time": run_model_result.execution_time,
                        "run_status": run_model_result.status,
                        "run_skipped": run_model_result.skip,
                        "run_error": run_model_result.error,
                        "model_materialization": run_model_result.model['materialized'],
                        "model_id": run_model_result.model.hashed_name(),
                        "hashed_contents": run_model_result.model.hashed_contents(),
                    })

                    if run_model_result.errored:
                        on_failure(run_model_result.model)
                        print(run_model_result.error)

            while model_index < num_models_this_batch:
                local_models = []
                for i in range(model_index, min(model_index + threads, num_models_this_batch)):
                    model = models_to_execute[i]
                    local_models.append(model)
                    msg = runner.pre_run_msg(model)
                    self.print_fancy_output_line(msg, 'RUN', get_idx(model), num_models)

                wrapped_models_to_execute = [{"runner": runner, "model": model} for model in local_models]
                map_result = pool.map_async(self.safe_execute_model, wrapped_models_to_execute, callback=on_complete)
                map_result.wait()
                run_model_results = map_result.get()

                model_index += threads

        pool.close()
        pool.join()

        print()
        print(runner.post_run_all_msg(model_results))
        runner.post_run_all(self.schema, model_results)

        return model_results

    def run_from_graph(self, runner, limit_to):
        print("Loading dependency graph file")
        linker = self.deserialize_graph()
        compiled_models = [make_compiled_model(fqn, linker.get_node(fqn)) for fqn in linker.nodes()]
        relevant_compiled_models = [m for m in compiled_models if m.is_type(runner.run_type)]

        for m in relevant_compiled_models:
            if m.should_execute():
                context = self.context.copy()
                context.update(m.context())
                m.compile(context)

        schema_name = self.target.schema


        print("Connecting to redshift")
        try:
            self.schema.create_schema_if_not_exists(schema_name)
        except psycopg2.OperationalError as e:
            print("ERROR: Could not connect to the target database. Try `dbt debug` for more information")
            print(str(e))
            sys.exit(1)

        existing = self.schema.query_for_existing(schema_name);

        if limit_to is None:
            specified_models = None
        else:
            specified_models = [find_model_by_name(relevant_compiled_models, name).fqn for name in limit_to]
        model_dependency_list = self.as_concurrent_dep_list(linker, relevant_compiled_models, existing, self.target, specified_models)

        on_failure = self.on_model_failure(linker, relevant_compiled_models)
        results = self.execute_models(runner, model_dependency_list, on_failure)

        return results

    def safe_run_from_graph(self, *args, **kwargs):
        try:
            return self.run_from_graph(*args, **kwargs)
        except:
            raise
        finally:
            if self.target.should_open_tunnel():
                print("Closing SSH tunnel... ", end="")
                sys.stdout.flush()
                self.target.cleanup()
                print("Done")

    # ------------------------------------

    def run_tests(self, limit_to=None):
        runner = TestRunner()
        return self.safe_run_from_graph(runner, limit_to)

    def run(self, limit_to=None):
        runner = ModelRunner()
        return self.safe_run_from_graph(runner, limit_to)

    def dry_run(self, limit_to=None):
        runner = DryRunner()
        return self.safe_run_from_graph(runner, limit_to)

    def run_archive(self):
        runner = ArchiveRunner()
        return self.safe_run_from_graph(runner, None)


