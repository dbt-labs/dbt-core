from typing import Any, Optional
from pathlib import Path
import dbt.utils
import dbt.deprecations
import dbt.exceptions

from dbt.config.renderer import DbtProjectYamlRenderer
from dbt.deps.base import downloads_directory
from dbt.deps.resolver import resolve_packages
from dbt.deps.registry import RegistryPinnedPackage

from dbt.events.functions import fire_event
from dbt.events.types import (
    DepsNoPackagesFound,
    DepsStartPackageInstall,
    DepsUpdateAvailable,
    DepsUpToDate,
    DepsInstallInfo,
    DepsListSubdirectory,
    DepsNotifyUpdatesAvailable,
    Formatting,
)
from dbt.clients import system

from dbt.task.base import BaseTask, move_to_nearest_project_dir

from dbt.config import Project


class DepsTask(BaseTask):
    def __init__(self, args: Any, project: Project) -> None:
        # N.B. This is a temporary fix for a bug when using relative paths via
        # --project-dir with deps.  A larger overhaul of our path handling methods
        # is needed to fix this the "right" way.
        # See GH-7615
        project.project_root = str(Path(project.project_root).resolve())

        move_to_nearest_project_dir(project.project_root)
        super().__init__(args=args, config=None, project=project)
        self.cli_vars = args.vars

    def track_package_install(
        self, package_name: str, source_type: str, version: Optional[str]
    ) -> None:
        # Hub packages do not need to be hashed, as they are public
        if source_type == "local":
            package_name = dbt.utils.md5(package_name)
            version = "local"
        elif source_type == "tarball":
            package_name = dbt.utils.md5(package_name)
            version = "tarball"
        elif source_type != "hub":
            package_name = dbt.utils.md5(package_name)
            version = dbt.utils.md5(version)

        dbt.tracking.track_package_install(
            "deps",
            self.project.hashed_name(),
            {"name": package_name, "source": source_type, "version": version},
        )

    def run(self) -> None:
        system.make_directory(self.project.packages_install_path)
        packages = self.project.packages.packages
        if not packages:
            fire_event(DepsNoPackagesFound())
            return

        with downloads_directory():
            final_deps = resolve_packages(packages, self.project, self.cli_vars)

            renderer = DbtProjectYamlRenderer(None, self.cli_vars)

            packages_to_upgrade = []
            for package in final_deps:
                package_name = package.name
                source_type = package.source_type()
                version = package.get_version()

                fire_event(DepsStartPackageInstall(package_name=package_name))
                package.install(self.project, renderer)
                fire_event(DepsInstallInfo(version_name=package.nice_version_name()))
                if isinstance(package, RegistryPinnedPackage):
                    version_latest = package.get_version_latest()
                    if version_latest != version:
                        packages_to_upgrade.append(package_name)
                        fire_event(DepsUpdateAvailable(version_latest=version_latest))
                    else:
                        fire_event(DepsUpToDate())
                if package.get_subdirectory():
                    fire_event(DepsListSubdirectory(subdirectory=package.get_subdirectory()))

                self.track_package_install(
                    package_name=package_name, source_type=source_type, version=version
                )
            if packages_to_upgrade:
                fire_event(Formatting(""))
                fire_event(DepsNotifyUpdatesAvailable(packages=packages_to_upgrade))
