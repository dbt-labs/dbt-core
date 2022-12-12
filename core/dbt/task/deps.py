from typing import Dict, Any

from dbt import flags

import dbt.utils
import dbt.deprecations
import dbt.exceptions

from dbt.config.profile import read_user_config
from dbt.config.runtime import load_project, UnsetProfile
from dbt.config.renderer import DbtProjectYamlRenderer
from dbt.config.utils import parse_cli_vars
from dbt.deps.base import downloads_directory
from dbt.deps.resolver import resolve_packages

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


from dbt.config import Project
from dbt.task.base import NoneConfig


class DepsTask(BaseTask):
    ConfigType = NoneConfig

    def __init__(
        self,
        args: Any,
        project: Project,
        cli_vars: Dict[str, Any],
    ):
        super().__init__(args=args, config=None, project=project)
        self.cli_vars = cli_vars

    def track_package_install(self, package_name: str, source_type: str, version: str) -> None:
        # Hub packages do not need to be hashed, as they are public
        # Use the string 'local' for local package versions
        if source_type == "local":
            package_name = dbt.utils.md5(package_name)
            version = "local"
        elif source_type != "hub":
            package_name = dbt.utils.md5(package_name)
            version = dbt.utils.md5(version)
        dbt.tracking.track_package_install(
            "deps",
            self.project.hashed_name(),
            {"name": package_name, "source": source_type, "version": version},
        )

    def run(self):
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
                if source_type == "hub":
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
                fire_event(DepsNotifyUpdatesAvailable(packages=packages_to_upgrade))

    @classmethod
    def _get_unset_profile(cls) -> UnsetProfile:
        profile = UnsetProfile()
        # The profile (for warehouse connection) is not needed, but we want
        # to get the UserConfig, which is also in profiles.yml
        user_config = read_user_config(flags.PROFILES_DIR)
        profile.user_config = user_config
        return profile

    @classmethod
    def from_args(cls, args):
        # deps needs to move to the project directory, as it does put files
        # into the modules directory
        nearest_project_dir = move_to_nearest_project_dir(args.project_dir)

        # N.B. parse_cli_vars is embedded into the param when using click.
        # replace this with:
        # cli_vars: Dict[str, Any] = getattr(args, "vars", {})
        # when this task is refactored for click
        cli_vars: Dict[str, Any] = parse_cli_vars(getattr(args, "vars", "{}"))
        project_root: str = args.project_dir or nearest_project_dir
        profile: UnsetProfile = cls._get_unset_profile()
        project = load_project(project_root, args.version_check, profile, cli_vars)

        return cls(args, project, cli_vars)

    @classmethod
    def from_project(cls, project: Project, cli_vars: Dict[str, Any]) -> "DepsTask":
        move_to_nearest_project_dir(project.project_root)
        # TODO: remove args=None once BaseTask does not require args
        return cls(None, project, cli_vars)
