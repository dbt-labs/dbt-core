from pathlib import Path
from typing import Optional

from dbt.artifacts.exceptions import IncompatibleSchemaError
from dbt.artifacts.schemas.freshness import FreshnessExecutionResultArtifact
from dbt.artifacts.schemas.manifest import WritableManifest
from dbt.artifacts.schemas.run import RunResultsArtifact
from dbt.constants import RUN_RESULTS_FILE_NAME
from dbt.contracts.graph.manifest import Manifest
from dbt.events.types import WarnStateTargetEqual
from dbt_common.events.functions import fire_event


def load_result_state(results_path) -> Optional[RunResultsArtifact]:
    if results_path.exists() and results_path.is_file():
        try:
            return RunResultsArtifact.read_and_check_versions(str(results_path))
        except IncompatibleSchemaError as exc:
            exc.add_filename(str(results_path))
            raise
    return None


class PreviousState:
    def __init__(self, state_path: Path, target_path: Path, project_root: Path) -> None:
        self.state_path: Path = state_path
        self.target_path: Path = target_path
        self.project_root: Path = project_root
        self.manifest: Optional[Manifest] = None
        self.results: Optional[RunResultsArtifact] = None
        self.sources: Optional[FreshnessExecutionResultArtifact] = None
        self.sources_current: Optional[FreshnessExecutionResultArtifact] = None

        if self.state_path == self.target_path:
            fire_event(WarnStateTargetEqual(state_path=str(self.state_path)))

        # Note: if state_path is absolute, project_root will be ignored.
        manifest_path = self.project_root / self.state_path / "manifest.json"
        if manifest_path.exists() and manifest_path.is_file():
            try:
                writable_manifest = WritableManifest.read_and_check_versions(str(manifest_path))
                self.manifest = Manifest.from_writable_manifest(writable_manifest)
            except IncompatibleSchemaError as exc:
                exc.add_filename(str(manifest_path))
                raise

        results_path = self.project_root / self.state_path / RUN_RESULTS_FILE_NAME
        self.results = load_result_state(results_path)

        sources_path = self.project_root / self.state_path / "sources.json"
        if sources_path.exists() and sources_path.is_file():
            try:
                self.sources = FreshnessExecutionResultArtifact.read_and_check_versions(
                    str(sources_path)
                )
            except IncompatibleSchemaError as exc:
                exc.add_filename(str(sources_path))
                raise

        sources_current_path = self.project_root / self.target_path / "sources.json"
        if sources_current_path.exists() and sources_current_path.is_file():
            try:
                self.sources_current = FreshnessExecutionResultArtifact.read_and_check_versions(
                    str(sources_current_path)
                )
            except IncompatibleSchemaError as exc:
                exc.add_filename(str(sources_current_path))
                raise
