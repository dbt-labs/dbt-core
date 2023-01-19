from datetime import datetime
from typing import Dict, Any
import traceback

import agate

from .runnable import ManifestTask

import dbt.exceptions
from dbt.adapters.factory import get_adapter
from dbt.config.utils import parse_cli_vars
from dbt.contracts.results import RunOperationResultsArtifact
from dbt.exceptions import DbtInternalError
from dbt.events.functions import fire_event
from dbt.events.types import (
    RunningOperationCaughtError,
    RunningOperationUncaughtError,
    LogDebugStackTrace,
)


class RunOperationTask(ManifestTask):
    def _get_macro_parts(self):
        macro_name = self.args.macro
        if "." in macro_name:
            package_name, macro_name = macro_name.split(".", 1)
        else:
            package_name = None

        return package_name, macro_name

    def _get_kwargs(self) -> Dict[str, Any]:
        return parse_cli_vars(self.args.args)

    def compile_manifest(self) -> None:
        if self.manifest is None:
            raise DbtInternalError("manifest was None in compile_manifest")

    def _run_unsafe(self) -> agate.Table:
        adapter = get_adapter(self.config)

        package_name, macro_name = self._get_macro_parts()
        macro_kwargs = self._get_kwargs()

        with adapter.connection_named("macro_{}".format(macro_name)):
            adapter.clear_transaction()
            res = adapter.execute_macro(
                macro_name, project=package_name, kwargs=macro_kwargs, manifest=self.manifest
            )

        return res

    def run(self) -> RunOperationResultsArtifact:
        start = datetime.utcnow()
        self._runtime_initialize()
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
