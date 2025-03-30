import json
from hashlib import sha1
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml

import dbt.deprecations
import dbt.exceptions
import dbt.utils
from dbt.config import Project
from dbt.config.project import load_yml_dict, package_config_from_data
from dbt.config.renderer import PackageRenderer
from dbt.constants import PACKAGE_LOCK_FILE_NAME, PACKAGE_LOCK_HASH_KEY
from dbt.contracts.project import PackageSpec
from dbt.deps.base import downloads_directory
from dbt.deps.registry import RegistryPinnedPackage
from dbt.deps.resolver import resolve_lock_packages, resolve_packages
from dbt.events.types import (
    DepsAddPackage,
    DepsFoundDuplicatePackage,
    DepsInstallInfo,
    DepsListSubdirectory,
    DepsLockUpdating,
    DepsNoPackagesFound,
    DepsNotifyUpdatesAvailable,
    DepsStartPackageInstall,
    DepsUpdateAvailable,
    DepsUpToDate,
)
from dbt.task.base import BaseTask, move_to_nearest_project_dir
from dbt_common.clients import system
from dbt_common.events.functions import fire_event
from dbt_common.events.types import Formatting


class dbtPackageDumper(yaml.Dumper):
    def increase_indent(self, flow=False, indentless=False):
        return super(dbtPackageDumper, self).increase_indent(flow, False)


def _create_sha1_hash(packages: List[PackageSpec]) -> str:
    """Create a SHA1 hash of the packages list,
    this is used to determine if the packages for current execution matches
    the previous lock.

    Args:
        list[Packages]: list of packages specified that are already rendered

    Returns:
        str: SHA1 hash of the packages list
    """
    package_strs = [json.dumps(package.to_dict(), sort_keys=True) for package in packages]
    package_strs = sorted(package_strs)

    return sha1("\n".join(package_strs).encode("utf-8")).hexdigest()


def _create_packages_yml_entry(package: str, version: Optional[str], source: str) -> dict:
    """Create a formatted entry to add to `packages.yml` or `package-lock.yml` file

    Args:
        package (str): Name of package to download
        version (str): Version of package to download
        source (str): Source of where to download package from

    Returns:
        dict: Formatted dict to write to `packages.yml` or `package-lock.yml` file
    """
    package_key = source
    version_key = "version"

    if source == "hub":
        package_key = "package"

    packages_yml_entry = {package_key: package}

    if source == "git":
        version_key = "revision"

    if version:
        if "," in version:
            version = version.split(",")  # type: ignore

        packages_yml_entry[version_key] = version

    return packages_yml_entry


class DepsTask(BaseTask):
    def __init__(self, args: Any, project: Project) -> None:
        super().__init__(args=args)
        # N.B. This is a temporary fix for a bug when using relative paths via
        # --project-dir with deps.  A larger overhaul of our path handling methods
        # is needed to fix this the "right" way.
        # See GH-7615
        project.project_root = str(Path(project.project_root).resolve())
        self.project = project
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

    def check_for_duplicate_packages(self, packages_yml):
        """Loop through contents of `packages.yml` to ensure no duplicate package names + versions.

        This duplicate check will take into consideration exact match of a package name, as well as
        a check to see if a package name exists within a name (i.e. a package name inside a git URL).

        Args:
            packages_yml (dict): In-memory read of `packages.yml` contents

        Returns:
            dict: Updated or untouched packages_yml contents
        """
        for i, pkg_entry in enumerate(packages_yml["packages"]):
            for val in pkg_entry.values():
                if self.args.add_package["name"] in val:
                    del packages_yml["packages"][i]

                    fire_event(DepsFoundDuplicatePackage(removed_package=pkg_entry))

        return packages_yml

    def add(self):
        packages_yml_filepath = (
            f"{self.project.project_root}/{self.project.packages_specified_path}"
        )
        if not system.path_exists(packages_yml_filepath):
            with open(packages_yml_filepath, "w") as package_yml:
                yaml.safe_dump({"packages": []}, package_yml)
            fire_event(Formatting("Created packages.yml"))

        new_package_entry = _create_packages_yml_entry(
            self.args.add_package["name"], self.args.add_package["version"], self.args.source
        )

        with open(packages_yml_filepath, "r") as user_yml_obj:
            packages_yml = yaml.safe_load(user_yml_obj)
            packages_yml = self.check_for_duplicate_packages(packages_yml)
            packages_yml["packages"].append(new_package_entry)

        self.project.packages.packages = package_config_from_data(packages_yml).packages

        if packages_yml:
            with open(packages_yml_filepath, "w") as pkg_obj:
                pkg_obj.write(
                    yaml.dump(packages_yml, Dumper=dbtPackageDumper, default_flow_style=False)
                )

                fire_event(
                    DepsAddPackage(
                        package_name=self.args.add_package["name"],
                        version=self.args.add_package["version"],
                        packages_filepath=packages_yml_filepath,
                    )
                )

    def lock(self) -> None:
        lock_filepath = f"{self.project.project_root}/{PACKAGE_LOCK_FILE_NAME}"

        packages = self.project.packages.packages
        packages_installed: Dict[str, Any] = {"packages": []}

        if not packages:
            fire_event(DepsNoPackagesFound())
            return

        with downloads_directory():
            resolved_deps = resolve_packages(packages, self.project, self.cli_vars)

        # this loop is to create the package-lock.yml in the same format as original packages.yml
        # package-lock.yml includes both the stated packages in packages.yml along with dependent packages
        for package in resolved_deps:
            packages_installed["packages"].append(package.to_dict())
        packages_installed[PACKAGE_LOCK_HASH_KEY] = _create_sha1_hash(
            self.project.packages.packages
        )

        with open(lock_filepath, "w") as lock_obj:
            yaml.dump(packages_installed, lock_obj, Dumper=dbtPackageDumper)

        fire_event(DepsLockUpdating(lock_filepath=lock_filepath))

    def run(self) -> None:
        move_to_nearest_project_dir(self.args.project_dir)
        if self.args.add_package:
            self.add()

        # Check lock file exist and generated by the same pacakges.yml
        # or dependencies.yml.
        lock_file_path = f"{self.project.project_root}/{PACKAGE_LOCK_FILE_NAME}"
        if not system.path_exists(lock_file_path):
            self.lock()
        elif self.args.upgrade:
            self.lock()
        else:
            # Check dependency definition is modified or not.
            current_hash = _create_sha1_hash(self.project.packages.packages)
            previous_hash = load_yml_dict(lock_file_path).get(PACKAGE_LOCK_HASH_KEY, None)
            if previous_hash != current_hash:
                self.lock()

        # Early return when 'dbt deps --lock'
        # Just resolve packages and write lock file, don't actually install packages
        if self.args.lock:
            return

        if system.path_exists(self.project.packages_install_path):
            system.rmtree(self.project.packages_install_path)

        system.make_directory(self.project.packages_install_path)

        packages_lock_dict = load_yml_dict(f"{self.project.project_root}/{PACKAGE_LOCK_FILE_NAME}")

        renderer = PackageRenderer(self.cli_vars)
        packages_lock_config = package_config_from_data(
            renderer.render_data(packages_lock_dict), packages_lock_dict
        ).packages

        if not packages_lock_config:
            fire_event(DepsNoPackagesFound())
            return

        with downloads_directory():
            lock_defined_deps = resolve_lock_packages(packages_lock_config)
            renderer = PackageRenderer(self.cli_vars)

            packages_to_upgrade = []

            for package in lock_defined_deps:
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
