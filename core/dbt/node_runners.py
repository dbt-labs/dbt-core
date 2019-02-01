from dbt.logger import GLOBAL_LOGGER as logger
from dbt.exceptions import NotImplementedException
from dbt.utils import get_nodes_by_tags
from dbt.node_types import NodeType, RunHookType
from dbt.adapters.factory import get_adapter
from dbt.contracts.results import RunModelResult, collect_timing_info
from dbt.compilation import compile_node

import dbt.clients.jinja
import dbt.context.runtime
import dbt.utils
import dbt.tracking
import dbt.ui.printer
import dbt.flags
import dbt.schema
import dbt.writer

import six
import sys
import threading
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
        "timing": run_model_result.timing,
    })


class BaseRunner(object):
    DefaultResult = RunModelResult
    def __init__(self, config, adapter, node, node_index, num_nodes):
        self.config = config
        self.adapter = adapter
        self.node = node
        self.node_index = node_index
        self.num_nodes = num_nodes

        self.skip = False
        self.skip_cause = None

    def run_with_hooks(self, manifest):
        if self.skip:
            return self.on_skip()

        # no before/after printing for ephemeral mdoels
        if not self.node.is_ephemeral_model:
            self.before_execute()

        result = self.safe_run(manifest)

        if not self.node.is_ephemeral_model:
            self.after_execute(result)

        return result

    def safe_run(self, manifest):
        catchable_errors = (dbt.exceptions.CompilationException,
                            dbt.exceptions.RuntimeException)

        result = self.DefaultResult(self.node)
        started = time.time()

        try:
            timing = []

            with collect_timing_info('compile') as timing_info:
                # if we fail here, we still have a compiled node to return
                # this has the benefit of showing a build path for the errant
                # model
                compiled_node = self.compile(manifest)
                result.node = compiled_node

            timing.append(timing_info)

            # for ephemeral nodes, we only want to compile, not run
            if not self.node.is_ephemeral_model:
                with collect_timing_info('execute') as timing_info:
                    result = self.run(compiled_node, manifest)

                timing.append(timing_info)

            for item in timing:
                result = result.add_timing_info(item)

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
            prefix = "Unhandled error while executing {filepath}".format(
                        filepath=self.node.build_path)

            error = "{prefix}\n{error}".format(
                         prefix=dbt.ui.printer.red(prefix),
                         error=str(e).strip())

            logger.error(error)
            logger.debug('', exc_info=True)
            result.error = dbt.compat.to_string(e)
            result.status = 'ERROR'

        finally:
            exc_str = self._safe_release_connection()

            # if releasing failed and the result doesn't have an error yet, set
            # an error
            if exc_str is not None and result.error is None:
                result.error = exc_str
                result.status = 'ERROR'

        result.execution_time = time.time() - started
        result.thread_id = threading.current_thread().name
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
        return self.skip_cause.node.is_ephemeral_model

    def on_skip(self):
        schema_name = self.node.schema
        node_name = self.node.name

        error = None
        if not self.node.is_ephemeral_model:
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


class CompileRunner(BaseRunner):
    def before_execute(self):
        pass

    def after_execute(self, result):
        pass

    def execute(self, compiled_node, manifest):
        return RunModelResult(compiled_node)

    def compile(self, manifest):
        return compile_node(self.adapter, self.config, self.node, manifest, {})


class ModelRunner(CompileRunner):
    def describe_node(self):
        materialization = dbt.utils.get_materialization(self.node)
        return "{0} model {1.database}.{1.schema}.{1.alias}".format(
            materialization, self.node
        )

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
        res, table = self.adapter.execute(
            test.wrapped_sql,
            model_name=test.name,
            auto_begin=True,
            fetch=True)

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
    def describe_node(self):
        cfg = self.node.get('config', {})
        return "archive {source_database}.{source_schema}.{source_table} --> "\
               "{target_database}.{target_schema}.{target_table}".format(**cfg)

    def print_result_line(self, result):
        dbt.ui.printer.print_archive_result_line(result, self.node_index,
                                                 self.num_nodes)


class SeedRunner(ModelRunner):
    def describe_node(self):
        return "seed file {0.database}.{0.schema}.{0.alias}".format(self.node)

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
