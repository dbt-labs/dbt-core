from datetime import datetime
import traceback

import agate

from .base import ConfiguredTask

import dbt.exceptions
from dbt.adapters.factory import get_adapter
from dbt.contracts.results import RunOperationResultsArtifact
from dbt.events.functions import fire_event
from dbt.events.types import (
    RunningOperationCaughtError,
    RunningOperationUncaughtError,
    LogDebugStackTrace,
)


class RunOperationTask(ConfiguredTask):
    def _get_macro_parts(self):
        macro_name = self.args.macro
        if "." in macro_name:
            package_name, macro_name = macro_name.split(".", 1)
        else:
            package_name = None

        return package_name, macro_name

    def _run_unsafe(self) -> agate.Table:
        adapter = get_adapter(self.config)

        package_name, macro_name = self._get_macro_parts()
        macro_kwargs = self.args.args

        with adapter.connection_named("macro_{}".format(macro_name)):
            adapter.clear_transaction()
            res = adapter.execute_macro(
                macro_name, project=package_name, kwargs=macro_kwargs, manifest=self.manifest
            )

        return res

    def run(self) -> RunOperationResultsArtifact:
        start = datetime.utcnow()
        self.compile_manifest()
        try:
            self._run_unsafe()
        except dbt.exceptions.Exception as exc:
            fire_event(RunningOperationCaughtError(exc=str(exc)))
            fire_event(LogDebugStackTrace(exc_info=traceback.format_exc()))
            success = False
        except Exception as exc:
            fire_event(RunningOperationUncaughtError(exc=str(exc)))
            fire_event(LogDebugStackTrace(exc_info=traceback.format_exc()))
            success = False
        else:
            success = True
        end = datetime.utcnow()
        return RunOperationResultsArtifact.from_success(
            generated_at=end,
            elapsed_time=(end - start).total_seconds(),
            success=success,
        )

    def interpret_results(self, results):
        return results.success
