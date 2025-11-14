import json
import os
import subprocess
from typing import List, Union

from dbt.artifacts.schemas.catalog import CatalogArtifact
from dbt.artifacts.schemas.run import RunExecutionResult
from dbt.cli.main import dbtRunner, dbtRunnerResult
from dbt.contracts.graph.manifest import Manifest, WritableManifest


class FusionRunnerException(Exception):
    pass


class FusionRunner(dbtRunner):
    def invoke(self, args: List[str], **kwargs) -> dbtRunnerResult:
        fs_bin_path = os.environ.get("FUSION_BINARY_PATH_TESTING")
        if not fs_bin_path:
            raise ValueError("FUSION_BINARY_PATH_TESTING is not set")

        fs_args = [fs_bin_path] + args
        fs_result = subprocess.run(fs_args, capture_output=True, text=True)

        dbt_result = self._build_dbt_runner_result(args, fs_result)

        return dbt_result

    def _build_dbt_runner_result(
        self, args: List[str], fs_result: subprocess.CompletedProcess
    ) -> dbtRunnerResult:
        # Return early on error, do not try to build the result
        if fs_result.returncode != 0:
            return dbtRunnerResult(
                success=False,
                exception=FusionRunnerException(fs_result.stderr) if fs_result.stderr else None,
                result=None,
            )

        result: Union[
            bool,  # debug
            CatalogArtifact,  # docs generate
            List[str],  # list/ls
            Manifest,  # parse
            None,  # clean, deps, init, source
            RunExecutionResult,  # build, compile, run, seed, snapshot, test, run-operation
        ] = None

        project_dir_index = args.index("--project-dir") if "--project-dir" in args else None
        project_dir = args[project_dir_index + 1] if project_dir_index is not None else os.getcwd()

        if "parse" in args:
            manifest_path = os.path.join(project_dir, "target", "manifest.json")
            if os.path.exists(manifest_path):
                with open(manifest_path, "r") as f:
                    manifest_json = json.load(f)
                    writable_manifest = WritableManifest.from_dict(manifest_json)
                result = Manifest.from_writable_manifest(writable_manifest)
        elif "list" in args or "ls" in args:
            breakpoint()
            result = []

        return dbtRunnerResult(
            success=True,
            exception=None,
            result=result,
        )
