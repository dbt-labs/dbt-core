import os
import subprocess
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple, Type

from dbt.artifacts.schemas.base import ArtifactMixin
from dbt.artifacts.schemas.catalog import CatalogArtifact
from dbt.artifacts.schemas.freshness import FreshnessExecutionResultArtifact
from dbt.artifacts.schemas.manifest import WritableManifest
from dbt.artifacts.schemas.run import RunResultsArtifact


FUSION_BINARY_PATH_TESTING = os.environ.get("FUSION_BINARY_PATH_TESTING", "dbt")

# Maps dbtCommandResult attribute name -> (artifact filename, artifact class)
ARTIFACT_MAPPING: Dict[str, Tuple[str, Type[ArtifactMixin]]] = {
    "manifest": ("manifest.json", WritableManifest),
    "run_results": ("run_results.json", RunResultsArtifact),
    "catalog": ("catalog.json", CatalogArtifact),
    "sources": ("sources.json", FreshnessExecutionResultArtifact),
}

class dbtRunnerFsException(Exception):
    def __init__(self, message: Optional[str] = None, error_code: Optional[str] = None):
        self.message: Optional[str] = message
        self.error_code: Optional[str] = error_code

@dataclass
class dbtCommandResult:
    """Contains the result artifacts of a dbt command invocation."""

    manifest: Optional[WritableManifest] = None
    run_results: Optional[RunResultsArtifact] = None
    catalog: Optional[CatalogArtifact] = None
    sources: Optional[FreshnessExecutionResultArtifact] = None


@dataclass
class dbtRunnerResult:
    """Contains the result of an invocation of the dbtRunner."""

    success: bool
    stdout: Optional[str] = None
    # TODO: consider having exceptions as a list instead since fs can return more than one
    exception: Optional[dbtRunnerFsException] = None
    warnings: List[str] = None
    result: Optional[dbtCommandResult] = None


class dbtRunnerFs:
    """Programmatic interface for invoking dbt commands via the fusion binary.

    Mirrors the dbt Core programmatic invocation API (dbtRunner.invoke),
    but delegates execution to an external dbt binary via subprocess.
    """

    def __init__(self, binary_path: Optional[str] = None) -> None:
        self.binary_path = binary_path or FUSION_BINARY_PATH_TESTING

    def invoke(self, args: List[str], **kwargs) -> dbtRunnerResult:
        """Invoke a dbt command.

        Args:
            args: CLI arguments as a list of strings, e.g. ["run", "--select", "my_model"].
            **kwargs: Additional CLI flags as keyword arguments. Boolean True values
                are passed as flags (e.g. fail_fast=True -> --fail-fast). Other values
                are passed as --key value pairs. List values are joined with spaces.

        Returns:
            A dbtRunnerResult indicating success or failure.
        """
        cmd = [self.binary_path] + list(args)

        for key, value in kwargs.items():
            flag = "--" + key.replace("_", "-")
            if isinstance(value, bool):
                if value:
                    cmd.append(flag)
                else:
                    cmd.append(flag.replace("--", "--no-"))
            elif isinstance(value, list):
                cmd.append(flag)
                cmd.extend(str(v) for v in value)
            else:
                cmd.append(flag)
                cmd.append(str(value))

        # TODO: parse out
        # cmd += ["--otel-file-name", "telemetry.jsonl"]

        project_dir = self._infer_project_dir(cmd)

        try:
            proc = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
            )
            runner_result = dbtRunnerResult(success=proc.returncode == 0)
            if proc.stderr:
                runner_result.exception = dbtRunnerFsException(message=proc.stderr)
            
            if proc.stdout:
                runner_result.stdout = proc.stdout
            
            runner_result.result = self._load_command_result(project_dir)

            return runner_result
        except FileNotFoundError as e:
            return dbtRunnerResult(
                success=False,
                exception=e,
            )
        except Exception as e:
            return dbtRunnerResult(
                success=False,
                exception=e,
            )

    @staticmethod
    def _infer_project_dir(args: List[str]) -> str:
        """Determine the project directory from args, env var, or cwd.

        Parses --project-dir from the args list. Falls back to the
        DBT_PROJECT_DIR env var, then to the current working directory.
        """
        try:
            idx = args.index("--project-dir")
            if idx + 1 < len(args):
                return args[idx + 1]
        except ValueError:
            pass

        return os.environ.get("DBT_PROJECT_DIR", os.getcwd())

    def _load_command_result(self, project_dir: str) -> dbtCommandResult:
        """Load artifacts from the target directory into a dbtCommandResult."""
        command_result = dbtCommandResult()
        target_dir = os.path.join(project_dir, "target")

        for attr, (filename, artifact_cls) in ARTIFACT_MAPPING.items():
            path = os.path.join(target_dir, filename)
            if os.path.exists(path):
                try:
                    setattr(command_result, attr, artifact_cls.read_and_check_versions(path))
                except Exception as e:
                    pass

        return command_result
