import os
import subprocess
from typing import List, Union

from dbt.artifacts.schemas.catalog import CatalogArtifact
from dbt.artifacts.schemas.run import RunExecutionResult
from dbt.cli.main import dbtRunner, dbtRunnerResult
from dbt.contracts.graph.manifest import Manifest


class FusionRunnerException(Exception):
    pass


class FusionRunner(dbtRunner):
    def invoke(self, args: List[str], **kwargs) -> dbtRunnerResult:
        fs_bin_path = os.environ.get("FUSION_BINARY_PATH_TESTING")
        if not fs_bin_path:
            raise ValueError("FUSION_BINARY_PATH_TESTING is not set")

        fs_args = [fs_bin_path] + args
        fs_result = subprocess.run(fs_args, capture_output=True, text=True)

        dbt_result = self._build_dbt_runner_result(fs_result)
        breakpoint()

        return dbt_result

    def _build_dbt_runner_result(self, fs_result: subprocess.CompletedProcess) -> dbtRunnerResult:
        result: Union[
            bool,  # debug
            CatalogArtifact,  # docs generate
            List[str],  # list/ls
            Manifest,  # parse
            None,  # clean, deps, init, source
            RunExecutionResult,  # build, compile, run, seed, snapshot, test, run-operation
        ] = None

        return dbtRunnerResult(
            success=fs_result.returncode == 0,
            exception=FusionRunnerException(fs_result.stderr) if fs_result.stderr else None,
            result=result,
        )
