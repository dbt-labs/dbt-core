from dbt.logger import GLOBAL_LOGGER as logger
from dbt.task.runnable import ManifestTask
from dbt.adapters.factory import get_adapter

import dbt
import dbt.utils
import dbt.exceptions


class RunOperationTask(ManifestTask):
    def _get_macro_parts(self):
        macro_name = self.args.macro
        if '.' in macro_name:
            package_name, macro_name = macro_name.split(".", 1)
        else:
            package_name = None

        return package_name, macro_name

    def _get_kwargs(self):
        return dbt.utils.parse_cli_vars(self.args.args)

    def compile_manifest(self):
        # skip building a linker, but do make sure to build the flat graph
        self.manifest.build_flat_graph()

    def _run_unsafe(self):
        adapter = get_adapter(self.config)

        package_name, macro_name = self._get_macro_parts()
        macro_kwargs = self._get_kwargs()

        with adapter.connection_named('macro_{}'.format(macro_name)):
            adapter.clear_transaction()
            res = adapter.execute_macro(
                macro_name,
                project=package_name,
                kwargs=macro_kwargs,
                manifest=self.manifest
            )

        return res

    def run(self):
        self._runtime_initialize()
        try:
            result = self._run_unsafe()
        except dbt.exceptions.Exception as exc:
            logger.error(
                'Encountered an error while running operation: {}'
                .format(exc)
            )
            logger.debug('', exc_info=True)
            return False, None
        except Exception as exc:
            logger.error(
                'Encountered an uncaught exception while running operation: {}'
                .format(exc)
            )
            logger.debug('', exc_info=True)
            return False, None
        else:
            return True, result

    def interpret_results(self, results):
        success, _ = results
        return success
