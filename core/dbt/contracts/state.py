from pathlib import Path
from .graph.manifest import WritableManifest
from typing import Optional


class PreviousState:
    def __init__(self, path: Path):
        self.path: Path = path
        self.manifest: Optional[WritableManifest] = None

        manifest_path = self.path / 'manifest.json'
        if manifest_path.exists() and manifest_path.is_file():
            self.manifest = WritableManifest.read(str(manifest_path))
