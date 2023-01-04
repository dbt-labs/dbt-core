from typing import Optional

import dbt.utils
import dbt.deprecations
import dbt.exceptions

from dbt.config import UnsetProfileConfig
from dbt.config.renderer import DbtProjectYamlRenderer
from dbt.deps.base import downloads_directory
from dbt.deps.resolver import resolve_packages
from dbt.deps.registry import RegistryPinnedPackage

from dbt.events.proto_types import ListOfStrings
from dbt.events.functions import fire_event
from dbt.events.types import (
    DepsNoPackagesFound,
    DepsStartPackageInstall,
    DepsUpdateAvailable,
    DepsUpToDate,
    DepsInstallInfo,
    DepsListSubdirectory,
    DepsNotifyUpdatesAvailable,
    EmptyLine,
)
from dbt.clients import system

from dbt.task.base import BaseTask, move_to_nearest_project_dir


class DepsTask(BaseTask):
    ConfigType = UnsetProfileConfig

    def __init__(self, args, config: UnsetProfileConfig):
        super().__init__(args=args, config=config)

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
            self.config,
            self.config.args,
            {"name": package_name, "source": source_type, "version": version},
        )

    def run(self) -> None:
        system.make_directory(self.config.packages_install_path)
        packages = self.config.packages.packages
        if not packages:
            fire_event(DepsNoPackagesFound())
            return

        with downloads_directory():
            final_deps = resolve_packages(packages, self.config)

            renderer = DbtProjectYamlRenderer(self.config, self.config.cli_vars)

            packages_to_upgrade = []
            for package in final_deps:
                package_name = package.name
                source_type = package.source_type()
                version = package.get_version()

                fire_event(DepsStartPackageInstall(package_name=package_name))
                package.install(self.config, renderer)
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
                fire_event(EmptyLine())
                fire_event(DepsNotifyUpdatesAvailable(packages=ListOfStrings(packages_to_upgrade)))

    @classmethod
    def from_args(cls, args):
        # deps needs to move to the project directory, as it does put files
        # into the modules directory
        move_to_nearest_project_dir(args)
        return super().from_args(args)
