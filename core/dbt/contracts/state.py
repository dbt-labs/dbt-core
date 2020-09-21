from pathlib import Path
from .graph.manifest import WritableManifest
from typing import Optional
from dbt.exceptions import IncompatibleSchemaException


class PreviousState:
    def __init__(self, path: Path):
        self.path: Path = path
        self.manifest: Optional[WritableManifest] = None

        manifest_path = self.path / 'manifest.json'
        if manifest_path.exists() and manifest_path.is_file():
            try:
                self.manifest = WritableManifest.read(str(manifest_path))
            except IncompatibleSchemaException as exc:
                exc.add_filename(str(manifest_path))
                raise
