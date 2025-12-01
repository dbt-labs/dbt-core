import shutil
from pathlib import Path

from hatchling.builders.hooks.plugin.interface import BuildHookInterface


class LicenseCopyBuildHook(BuildHookInterface):
    """
    Copy LICENSE from repository root to the core directory during build.

    GitHub requires the LICENSE file to be at the repository root to properly
    detect and display license information. However, PyPI rejects distribution
    metadata with parent directory references (e.g., '../LICENSE').

    This build hook copies the LICENSE from the repository root into the build
    directory at build time, allowing the package metadata to reference a local
    LICENSE file without parent directory traversal, while keeping the original
    LICENSE at the root for GitHub detection.
    """

    def initialize(self, version, build_data):
        # Get the path to the LICENSE file in the parent directory
        root_license = Path(self.root) / ".." / "LICENSE"
        target_license = Path(self.root) / "LICENSE"

        # Copy LICENSE file if it doesn't exist or is outdated
        if root_license.exists():
            shutil.copy2(str(root_license.resolve()), str(target_license))
