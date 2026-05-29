import functools
import shutil
from pathlib import Path
from typing import Dict, List, Tuple

import dbt.exceptions
from dbt.clients import git
from dbt.config.project import load_yml_dict
from dbt.config.renderer import PackageRenderer
from dbt.constants import DEPENDENCIES_FILE_NAME
from dbt.deps.base import downloads_directory, get_downloads_path
from dbt.deps.registry import RegistryPinnedPackage
from dbt.events.types import DepsInstallInfo, DepsStartPackageInstall
from dbt_common.clients import system
from dbt_common.events.functions import fire_event
from dbt_common.utils.connection import connection_exception_retry


def resolve_skill_paths(skill: Dict) -> List[str]:
    """Build the list of install paths from the ``path`` field.

    - If ``path`` is specified, use those paths.
    - If ``path`` is not specified, default to .agents/skills/.
    - Deduplicate while preserving order.
    """
    paths: List[str] = []

    raw_paths = skill.get("path")
    if raw_paths:
        if isinstance(raw_paths, str):
            raw_paths = [raw_paths]
        for p in raw_paths:
            if p not in paths:
                paths.append(p)

    if not paths:
        paths = [".agents/skills"]

    return paths


def resolve_git(skill: Dict) -> Tuple[str, str]:
    """Clone a git skill repo and return (source_path, version_name)."""
    fire_event(DepsStartPackageInstall(package_name=skill["git"]))
    dir_ = git.clone_and_checkout(
        skill["git"],
        get_downloads_path(),
        revision=skill.get("revision"),
    )
    source = str(Path(get_downloads_path()) / dir_)
    revision = skill.get("revision", "HEAD")
    return source, f"revision {revision}"


def resolve_local(skill: Dict, project_root: str) -> Tuple[str, str]:
    """Resolve a local skill path and return (source_path, version_name)."""
    local_path = Path(skill["local"]).expanduser()
    if not local_path.is_absolute():
        local_path = Path(project_root) / skill["local"]
    fire_event(DepsStartPackageInstall(package_name=str(local_path)))
    return str(local_path), f"local path {skill['local']}"


def resolve_registry(skill: Dict, project, cli_vars) -> Tuple[str, str]:
    """Download a registry package and return (source_path, version_name)."""
    package_name = skill["package"]
    version = str(skill["version"])
    fire_event(DepsStartPackageInstall(package_name=package_name))
    pkg = RegistryPinnedPackage(package=package_name, version=version, version_latest=version)
    renderer = PackageRenderer(cli_vars)
    try:
        metadata = pkg._fetch_metadata(project, renderer)
    except Exception:
        raise dbt.exceptions.DbtProjectError(
            f"Package {package_name} was not found in the package index. "
            "Is the package name correct?"
        )
    # Use metadata.name (e.g. "dbt_utils") for the extraction directory,
    # not the registry name (e.g. "dbt-labs/dbt-utils") which contains a slash.
    project_name = metadata.name
    tar_name = f"{project_name}.{version}.tar.gz"
    tar_path = (Path(get_downloads_path()) / tar_name).resolve(strict=False)
    system.make_directory(str(tar_path.parent))
    download_untar_fn = functools.partial(
        pkg.download_and_untar,
        metadata.downloads.tarball,
        str(tar_path),
        get_downloads_path(),
        project_name,
    )
    connection_exception_retry(download_untar_fn, 5)
    source = str(Path(get_downloads_path()) / project_name)
    return source, f"version {version}"


def install_skills(project, cli_vars) -> None:
    """Install skills defined under the ``skills`` key in dependencies.yml."""
    deps_path = str(Path(project.project_root) / DEPENDENCIES_FILE_NAME)
    if not system.path_exists(deps_path):
        return

    dependencies_dict = load_yml_dict(deps_path)
    skills = dependencies_dict.get("skills", [])
    if not skills:
        return

    with downloads_directory():
        for skill in skills:
            raw_paths = resolve_skill_paths(skill)

            if "git" in skill:
                source, version_name = resolve_git(skill)
            elif "local" in skill:
                source, version_name = resolve_local(skill, project.project_root)
            elif "package" in skill:
                source, version_name = resolve_registry(skill, project, cli_vars)
            else:
                continue

            source_path = Path(source)

            # Honor explicit subdirectory override
            subdirectory = skill.get("subdirectory")
            if subdirectory:
                candidate = source_path / subdirectory
                if candidate.is_dir():
                    source_path = candidate

            # Convention: if the source contains a skills/ subdirectory,
            # install from there instead of the root.
            skills_subdir = source_path / "skills"
            if skills_subdir.is_dir():
                source_path = skills_subdir

            for install_path in raw_paths:
                resolved = Path(install_path)
                if resolved.is_absolute():
                    dest = resolved
                else:
                    dest = Path(project.project_root) / install_path

                system.make_directory(str(dest))

                # Case 3: source itself is a single skill directory.
                if (source_path / "SKILL.md").is_file():
                    skill_name = source_path.name
                    entry_dest = dest / skill_name
                    if entry_dest.exists():
                        system.rmtree(str(entry_dest))
                    shutil.copytree(str(source_path), str(entry_dest))
                else:
                    # Cases 1 & 2: source contains skill subdirectories.
                    # Install each skill folder individually (must contain SKILL.md).
                    # This preserves existing skills in the destination that came
                    # from other repos.
                    allowed_skills = skill.get("skills")
                    for entry in source_path.iterdir():
                        if not entry.is_dir():
                            continue
                        if not (entry / "SKILL.md").is_file():
                            continue
                        if allowed_skills and entry.name not in allowed_skills:
                            continue

                        entry_dest = dest / entry.name
                        if entry_dest.exists():
                            system.rmtree(str(entry_dest))
                        shutil.copytree(str(entry), str(entry_dest))

                fire_event(DepsInstallInfo(version_name=f"{version_name} to {dest}"))
