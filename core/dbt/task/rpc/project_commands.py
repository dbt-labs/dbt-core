from datetime import datetime
from typing import List, Optional, Union

from dbt.contracts.graph.manifest import WritableManifest
from dbt.contracts.rpc import (
    GetManifestParameters,
    GetManifestResult,
    RPCCompileParameters,
    RPCDocsGenerateParameters,
    RPCRunOperationParameters,
    RPCSeedParameters,
    RPCTestParameters,
    RemoteCatalogResults,
    RemoteExecutionResult,
    RemoteRunOperationResult,
    RPCSnapshotParameters,
    RPCSourceFreshnessParameters,
)
from dbt.rpc.method import (
    Parameters, RemoteManifestMethod
)

from dbt.task.base import BaseTask
from dbt.task.compile import CompileTask
from dbt.task.freshness import FreshnessTask
from dbt.task.generate import GenerateTask
from dbt.task.run import RunTask
from dbt.task.run_operation import RunOperationTask
from dbt.task.seed import SeedTask
from dbt.task.snapshot import SnapshotTask
from dbt.task.test import TestTask

from .base import RPCTask
from .cli import HasCLI


class RPCCommandTask(
    RPCTask[Parameters],
    HasCLI[Parameters, RemoteExecutionResult],
    BaseTask,
):
    @staticmethod
    def _listify(
        value: Optional[Union[str, List[str]]]
    ) -> Optional[List[str]]:
        if value is None:
            return None
        elif isinstance(value, str):
            return [value]
        else:
            return value

    def handle_request(self) -> RemoteExecutionResult:
        return self.run()


class RemoteCompileProjectTask(
    RPCCommandTask[RPCCompileParameters], CompileTask
):
    METHOD_NAME = 'compile'

    def set_args(self, params: RPCCompileParameters) -> None:
        self.args.models = self._listify(params.models)
        self.args.exclude = self._listify(params.exclude)
        self.args.selector_name = params.selector
        if params.threads is not None:
            self.args.threads = params.threads


class RemoteRunProjectTask(RPCCommandTask[RPCCompileParameters], RunTask):
    METHOD_NAME = 'run'

    def set_args(self, params: RPCCompileParameters) -> None:
        self.args.models = self._listify(params.models)
        self.args.exclude = self._listify(params.exclude)
        self.args.selector_name = params.selector
        if params.threads is not None:
            self.args.threads = params.threads


class RemoteSeedProjectTask(RPCCommandTask[RPCSeedParameters], SeedTask):
    METHOD_NAME = 'seed'

    def set_args(self, params: RPCSeedParameters) -> None:
        # select has an argparse `dest` value of `models`.
        self.args.models = self._listify(params.select)
        self.args.exclude = self._listify(params.exclude)
        self.args.selector_name = params.selector
        if params.threads is not None:
            self.args.threads = params.threads
        self.args.show = params.show


class RemoteTestProjectTask(RPCCommandTask[RPCTestParameters], TestTask):
    METHOD_NAME = 'test'

    def set_args(self, params: RPCTestParameters) -> None:
        self.args.models = self._listify(params.models)
        self.args.exclude = self._listify(params.exclude)
        self.args.selector_name = params.selector
        self.args.data = params.data
        self.args.schema = params.schema
        if params.threads is not None:
            self.args.threads = params.threads


class RemoteDocsGenerateProjectTask(
    RPCCommandTask[RPCDocsGenerateParameters],
    GenerateTask,
):
    METHOD_NAME = 'docs.generate'

    def set_args(self, params: RPCDocsGenerateParameters) -> None:
        self.args.models = None
        self.args.exclude = None
        self.args.selector_name = None
        self.args.compile = params.compile

    def get_catalog_results(
        self, nodes, sources, generated_at, compile_results, errors
    ) -> RemoteCatalogResults:
        return RemoteCatalogResults(
            nodes=nodes,
            sources=sources,
            generated_at=datetime.utcnow(),
            _compile_results=compile_results,
            errors=errors,
            logs=[],
        )


class RemoteRunOperationTask(
    RunOperationTask,
    RemoteManifestMethod[RPCRunOperationParameters, RemoteRunOperationResult],
    HasCLI[RPCRunOperationParameters, RemoteRunOperationResult],
):
    METHOD_NAME = 'run-operation'

    def __init__(self, args, config, manifest):
        super().__init__(args, config)
        RemoteManifestMethod.__init__(
            self, args, config, manifest  # type: ignore
        )

    def load_manifest(self):
        # we started out with a manifest!
        pass

    def set_args(self, params: RPCRunOperationParameters) -> None:
        self.args.macro = params.macro
        self.args.args = params.args

    def _get_kwargs(self):
        if isinstance(self.args.args, dict):
            return self.args.args
        else:
            return RunOperationTask._get_kwargs(self)

    def _runtime_initialize(self):
        return RunOperationTask._runtime_initialize(self)

    def handle_request(self) -> RemoteRunOperationResult:
        base = RunOperationTask.run(self)
        result = RemoteRunOperationResult(
            results=base.results,
            generated_at=base.generated_at,
            logs=[],
            success=base.success,
            elapsed_time=base.elapsed_time
        )
        return result

    def interpret_results(self, results):
        return results.success


class RemoteSnapshotTask(RPCCommandTask[RPCSnapshotParameters], SnapshotTask):
    METHOD_NAME = 'snapshot'

    def set_args(self, params: RPCSnapshotParameters) -> None:
        # select has an argparse `dest` value of `models`.
        self.args.models = self._listify(params.select)
        self.args.exclude = self._listify(params.exclude)
        self.args.selector_name = params.selector
        if params.threads is not None:
            self.args.threads = params.threads


class RemoteSourceFreshnessTask(
    RPCCommandTask[RPCSourceFreshnessParameters],
    FreshnessTask
):
    METHOD_NAME = 'snapshot-freshness'

    def set_args(self, params: RPCSourceFreshnessParameters) -> None:
        self.args.selected = self._listify(params.select)
        if params.threads is not None:
            self.args.threads = params.threads
        self.args.output = None


# this is a weird and special method.
class GetManifest(
    RemoteManifestMethod[GetManifestParameters, GetManifestResult]
):
    METHOD_NAME = 'get-manifest'

    def set_args(self, params: GetManifestParameters) -> None:
        self.args.models = None
        self.args.exclude = None
        self.args.selector_name = None

    def handle_request(self) -> GetManifestResult:
        task = RemoteCompileProjectTask(self.args, self.config, self.manifest)
        task.handle_request()

        manifest: Optional[WritableManifest] = None
        if task.manifest is not None:
            manifest = task.manifest.writable_manifest()

        return GetManifestResult(
            logs=[],
            manifest=manifest,
        )

    def interpret_results(self, results):
        return results.manifest is not None
