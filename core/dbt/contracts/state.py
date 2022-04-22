from pathlib import Path
from .graph.manifest import WritableManifest
from .results import RunResultsArtifact
from .results import FreshnessExecutionResultArtifact
from typing import Optional
from dbt.exceptions import IncompatibleSchemaException


class PreviousState:
    def __init__(self, path: Path, current_path: Path):
        self.path: Path = path
        self.current_path: Path = current_path
        self.manifest: Optional[WritableManifest] = None
        self.results: Optional[RunResultsArtifact] = None
        self.sources: Optional[FreshnessExecutionResultArtifact] = None
        self.sources_current: Optional[FreshnessExecutionResultArtifact] = None

        manifest_path = self.path / "manifest.json"
        if manifest_path.exists() and manifest_path.is_file():
            try:
                self.manifest = WritableManifest.read_and_check_versions(str(manifest_path))
            except IncompatibleSchemaException as exc:
                exc.add_filename(str(manifest_path))
                raise

        results_path = self.path / "run_results.json"
        if results_path.exists() and results_path.is_file():
            try:
                self.results = RunResultsArtifact.read_and_check_versions(str(results_path))
            except IncompatibleSchemaException as exc:
                exc.add_filename(str(results_path))
                raise

        sources_path = self.path / "sources.json"
        if sources_path.exists() and sources_path.is_file():
            try:
                self.sources = FreshnessExecutionResultArtifact.read_and_check_versions(
                    str(sources_path)
                )
            except IncompatibleSchemaException as exc:
                exc.add_filename(str(sources_path))
                raise

        sources_current_path = self.current_path / "sources.json"
        if sources_current_path.exists() and sources_current_path.is_file():
            try:
                self.sources_current = FreshnessExecutionResultArtifact.read_and_check_versions(
                    str(sources_current_path)
                )
            except IncompatibleSchemaException as exc:
                exc.add_filename(str(sources_current_path))
                raise
