import json
from datetime import datetime
from pathlib import Path
from typing import List, Optional, Union

from dbt import flags
from dbt.contracts.graph.manifest import WritableManifest
from dbt.contracts.rpc import (
    GetManifestParameters,
    GetManifestResult,
    RPCCompileParameters,
    RPCDocsGenerateParameters,
    RPCRunParameters,
    RPCRunOperationParameters,
    RPCSeedParameters,
    RPCTestParameters,
    RemoteCatalogResults,
    RemoteExecutionResult,
    RemoteListResults,
    RemoteRunOperationResult,
    RPCSnapshotParameters,
    RPCSourceFreshnessParameters,
    RPCListParameters,
    RPCBuildParameters,
)
from dbt.exceptions import RuntimeException
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
from dbt.task.list import ListTask
from dbt.task.build import BuildTask

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


def state_path(state: Optional[str]) -> Optional[Path]:
    if state is not None:
        return Path(state)
    elif flags.ARTIFACT_STATE_PATH is not None:
        return Path(flags.ARTIFACT_STATE_PATH)
    else:
        return None


class RemoteCompileProjectTask(
    RPCCommandTask[RPCCompileParameters], CompileTask
):
    METHOD_NAME = 'compile'

    def set_args(self, params: RPCCompileParameters) -> None:
        if params.models:
            self.args.select = self._listify(params.models)
        else:
            self.args.select = self._listify(params.select)
        self.args.exclude = self._listify(params.exclude)
        self.args.selector_name = params.selector
        if params.threads is not None:
            self.args.threads = params.threads

        self.args.state = state_path(params.state)

        self.set_previous_state()


class RemoteRunProjectTask(RPCCommandTask[RPCRunParameters], RunTask):
    METHOD_NAME = 'run'

    def set_args(self, params: RPCRunParameters) -> None:
        if params.models:
            self.args.select = self._listify(params.models)
        else:
            self.args.select = self._listify(params.select)
        self.args.exclude = self._listify(params.exclude)
        self.args.selector_name = params.selector

        if params.threads is not None:
            self.args.threads = params.threads
        if params.defer is None:
            self.args.defer = flags.DEFER_MODE
        else:
            self.args.defer = params.defer

        self.args.state = state_path(params.state)
        self.set_previous_state()


class RemoteSeedProjectTask(RPCCommandTask[RPCSeedParameters], SeedTask):
    METHOD_NAME = 'seed'

    def set_args(self, params: RPCSeedParameters) -> None:
        # select has an argparse `dest` value of `models`.
        self.args.select = self._listify(params.select)
        self.args.exclude = self._listify(params.exclude)
        self.args.selector_name = params.selector
        if params.threads is not None:
            self.args.threads = params.threads
        self.args.show = params.show

        self.args.state = state_path(params.state)
        self.set_previous_state()


class RemoteTestProjectTask(RPCCommandTask[RPCTestParameters], TestTask):
    METHOD_NAME = 'test'

    def set_args(self, params: RPCTestParameters) -> None:
        if params.models:
            self.args.select = self._listify(params.models)
        else:
            self.args.select = self._listify(params.select)
        self.args.exclude = self._listify(params.exclude)
        self.args.selector_name = params.selector
        self.args.data = params.data
        self.args.schema = params.schema
        if params.threads is not None:
            self.args.threads = params.threads
        if params.defer is None:
            self.args.defer = flags.DEFER_MODE
        else:
            self.args.defer = params.defer

        self.args.state = state_path(params.state)
        self.set_previous_state()


class RemoteDocsGenerateProjectTask(
    RPCCommandTask[RPCDocsGenerateParameters],
    GenerateTask,
):
    METHOD_NAME = 'docs.generate'

    def set_args(self, params: RPCDocsGenerateParameters) -> None:
        self.args.select = None
        self.args.exclude = None
        self.args.selector_name = None
        self.args.compile = params.compile

        self.args.state = state_path(params.state)

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
        result = RemoteRunOperationResult.from_local_result(base=base, logs=[])
        return result

    def interpret_results(self, results):
        return results.success


class RemoteSnapshotTask(RPCCommandTask[RPCSnapshotParameters], SnapshotTask):
    METHOD_NAME = 'snapshot'

    def set_args(self, params: RPCSnapshotParameters) -> None:
        # select has an argparse `dest` value of `models`.
        self.args.select = self._listify(params.select)
        self.args.exclude = self._listify(params.exclude)
        self.args.selector_name = params.selector
        if params.threads is not None:
            self.args.threads = params.threads

        self.args.state = state_path(params.state)
        self.set_previous_state()


class RemoteSourceFreshnessTask(
    RPCCommandTask[RPCSourceFreshnessParameters],
    FreshnessTask
):
    METHOD_NAME = 'source-freshness'

    def set_args(self, params: RPCSourceFreshnessParameters) -> None:
        self.args.select = self._listify(params.select)
        self.args.exclude = self._listify(params.exclude)
        self.args.selector_name = params.selector
        if params.threads is not None:
            self.args.threads = params.threads
        self.args.output = None


class RemoteSourceSnapshotFreshnessTask(
    RemoteSourceFreshnessTask
):
    """ Deprecated task method name, aliases to `source-freshness` """
    METHOD_NAME = 'snapshot-freshness'


# this is a weird and special method.
class GetManifest(
    RemoteManifestMethod[GetManifestParameters, GetManifestResult]
):
    METHOD_NAME = 'get-manifest'

    def set_args(self, params: GetManifestParameters) -> None:
        self.args.select = None
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


class RemoteListTask(
    RPCCommandTask[RPCListParameters], ListTask
):
    METHOD_NAME = 'list'

    def set_args(self, params: RPCListParameters) -> None:
        self.args.output = params.output
        self.args.output_keys = params.output_keys
        self.args.resource_types = self._listify(params.resource_types)
        self.args.models = self._listify(params.models)
        self.args.exclude = self._listify(params.exclude)
        self.args.selector_name = params.selector
        self.args.select = self._listify(params.select)
        self.args.single_threaded = True

        if self.args.models:
            if self.args.select:
                raise RuntimeException(
                    '"models" and "select" are mutually exclusive arguments'
                )
            if self.args.resource_types:
                raise RuntimeException(
                    '"models" and "resource_type" are mutually exclusive '
                    'arguments'
                )

    @staticmethod
    def output_results(results):
        return RemoteListResults(
            output=[json.loads(x) for x in results],
            logs=None
        )


class RemoteBuildProjectTask(RPCCommandTask[RPCBuildParameters], BuildTask):

    METHOD_NAME = 'build'

    def set_args(self, params: RPCBuildParameters) -> None:
        self.args.resource_types = self._listify(params.resource_types)
        self.args.select = self._listify(params.select)
        self.args.exclude = self._listify(params.exclude)
        self.args.selector_name = params.selector

        if params.threads is not None:
            self.args.threads = params.threads
        if params.defer is None:
            self.args.defer = flags.DEFER_MODE
        else:
            self.args.defer = params.defer

        self.args.state = state_path(params.state)
        self.set_previous_state()
