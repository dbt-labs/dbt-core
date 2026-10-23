from argparse import Namespace
from contextlib import nullcontext
from pathlib import Path
from typing import Generator, Optional
from unittest import mock

import pytest
import yaml
from requests.exceptions import ConnectionError

from dbt.config import Project
from dbt.constants import PACKAGE_LOCK_HASH_KEY
from dbt.contracts.project import GitPackage, LocalPackage, PackageConfig
from dbt.deps.git import GitPinnedPackage
from dbt.events.types import DepsInstallInfo, DepsStartPackageInstall
from dbt.task.deps import DepsTask, _create_sha1_hash
from dbt_common.exceptions import DbtInternalError, DbtRuntimeError


def write_local_package(root: Path, path: str, name: str, packages: Optional[list] = None) -> Path:
    source = root / path
    source.mkdir(parents=True)
    (source / "dbt_project.yml").write_text(
        yaml.safe_dump({"name": name, "version": "1.0.0", "config-version": 2})
    )
    (source / "macros").mkdir()
    (source / "macros" / "example.sql").write_text("{% macro example() %}1{% endmacro %}")
    if packages:
        (source / "packages.yml").write_text(yaml.safe_dump({"packages": packages}))
    return source


@pytest.fixture
def deps_task(
    project: Project, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> Generator[DepsTask, None, None]:
    monkeypatch.chdir(tmp_path)
    (tmp_path / "dbt_project.yml").write_text("name: test_project\nversion: '1.0.0'\n")
    project.project_root = str(tmp_path)
    project.packages_install_path = str(tmp_path / "dbt_packages")
    project.packages = PackageConfig(
        [LocalPackage(local="local_macros"), GitPackage(git="https://example.com/remote.git")]
    )
    write_local_package(tmp_path, "local_macros", "local_macros")
    args = Namespace(
        vars={}, project_dir=str(tmp_path), add_package=None, lock=False, upgrade=False
    )
    with (
        mock.patch(
            "dbt.task.deps.downloads_directory",
            autospec=True,
            return_value=nullcontext(str(tmp_path / "downloads")),
        ),
        mock.patch("dbt.tracking.track_package_install", autospec=True),
    ):
        yield DepsTask(args, project)


@pytest.mark.parametrize("local_first", [True, False])
@pytest.mark.parametrize("phase", ["resolve", "install", "upgrade", "changed"])
def test_local_package_installs_after_remote_failure(
    deps_task: DepsTask, tmp_path: Path, local_first: bool, phase: str
) -> None:
    if not local_first:
        deps_task.project.packages.packages.reverse()
    lock_path = tmp_path / "package-lock.yml"
    if phase != "resolve":
        lock_packages = [
            {"local": "local_macros", "name": "local_macros"},
            {"git": "https://example.com/remote.git", "revision": "abc123", "name": "remote"},
        ]
        if not local_first:
            lock_packages.reverse()
        lock_path.write_text(
            yaml.safe_dump(
                {
                    "packages": lock_packages,
                    PACKAGE_LOCK_HASH_KEY: (
                        "old_hash"
                        if phase == "changed"
                        else _create_sha1_hash(deps_task.project.packages.packages)
                    ),
                }
            )
        )
    lock_before = lock_path.read_bytes() if lock_path.exists() else None
    deps_task.args.upgrade = phase == "upgrade"
    error = DbtInternalError("remote checkout failed")
    with (
        mock.patch.object(GitPinnedPackage, "_checkout", autospec=True, side_effect=error),
        mock.patch("dbt.task.deps.fire_event", autospec=True) as events,
    ):
        with pytest.raises(DbtInternalError) as exc:
            deps_task.run()
    assert exc.value is error
    installed = tmp_path / "dbt_packages" / "local_macros" / "macros" / "example.sql"
    assert installed.read_text() == "{% macro example() %}1{% endmacro %}"
    assert (tmp_path / "local_macros" / "dbt_project.yml").exists()
    assert (lock_path.read_bytes() if lock_path.exists() else None) == lock_before
    assert any(
        isinstance(call.args[0], DepsStartPackageInstall)
        and call.args[0].package_name == "local_macros"
        for call in events.call_args_list
    )
    assert any(
        isinstance(call.args[0], DepsInstallInfo)
        and call.args[0].version_name == "<local @ local_macros>"
        for call in events.call_args_list
    )


@pytest.mark.parametrize("error_type", [DbtRuntimeError, ConnectionError])
def test_local_fallback_preserves_remote_error(
    deps_task: DepsTask, tmp_path: Path, error_type: type
) -> None:
    error = error_type("remote unavailable")
    with mock.patch.object(GitPinnedPackage, "_checkout", autospec=True, side_effect=error):
        with pytest.raises(error_type) as exc:
            deps_task.run()
    assert exc.value is error
    assert (tmp_path / "dbt_packages" / "local_macros" / "dbt_project.yml").exists()


def test_lock_only_does_not_install_local_packages(deps_task: DepsTask, tmp_path: Path) -> None:
    deps_task.args.lock = True
    with mock.patch.object(
        GitPinnedPackage, "_checkout", autospec=True, side_effect=DbtInternalError("remote failed")
    ):
        with pytest.raises(DbtInternalError):
            deps_task.run()
    assert not (tmp_path / "dbt_packages").exists()


def test_local_fallback_skips_packages_with_remote_dependencies(
    deps_task: DepsTask, tmp_path: Path
) -> None:
    write_local_package(
        tmp_path,
        "dependent_macros",
        "dependent_macros",
        [{"git": "https://example.com/nested.git"}],
    )
    deps_task.project.packages.packages.insert(0, LocalPackage(local="dependent_macros"))
    with mock.patch.object(
        GitPinnedPackage, "_checkout", autospec=True, side_effect=DbtInternalError("remote failed")
    ):
        with pytest.raises(DbtInternalError):
            deps_task.run()
    assert (tmp_path / "dbt_packages" / "local_macros" / "dbt_project.yml").exists()
    assert not (tmp_path / "dbt_packages" / "dependent_macros").exists()


@pytest.mark.parametrize("invalid_local", ["missing", "duplicate", "root_name"])
def test_local_fallback_keeps_valid_packages_when_another_local_is_invalid(
    deps_task: DepsTask, tmp_path: Path, invalid_local: str
) -> None:
    if invalid_local != "missing":
        name = "test_project" if invalid_local == "root_name" else "duplicate"
        write_local_package(tmp_path, "invalid_local", name)
        if invalid_local == "duplicate":
            write_local_package(tmp_path, "other_duplicate", name)
            deps_task.project.packages.packages.append(LocalPackage(local="other_duplicate"))
    deps_task.project.packages.packages.insert(1, LocalPackage(local="invalid_local"))
    deps_task.project.packages.packages.reverse()
    error = DbtInternalError("remote failed")
    with mock.patch.object(GitPinnedPackage, "_checkout", autospec=True, side_effect=error):
        with pytest.raises(DbtInternalError) as exc:
            deps_task.run()
    assert exc.value is error
    assert (tmp_path / "dbt_packages" / "local_macros" / "dbt_project.yml").exists()
    assert not (tmp_path / "dbt_packages" / "duplicate").exists()
    assert not (tmp_path / "dbt_packages" / "test_project").exists()


def test_local_fallback_preserves_existing_packages(deps_task: DepsTask, tmp_path: Path) -> None:
    existing = tmp_path / "dbt_packages" / "cached_remote" / "dbt_project.yml"
    existing.parent.mkdir(parents=True)
    existing.write_text("name: cached_remote\n")
    with mock.patch.object(
        GitPinnedPackage, "_checkout", autospec=True, side_effect=DbtInternalError("remote failed")
    ):
        with pytest.raises(DbtInternalError):
            deps_task.run()
    assert existing.read_text() == "name: cached_remote\n"
    assert (tmp_path / "dbt_packages" / "local_macros" / "dbt_project.yml").exists()


def test_successful_local_install_still_writes_complete_lock_file(
    deps_task: DepsTask, tmp_path: Path
) -> None:
    deps_task.project.packages.packages = [LocalPackage(local="local_macros")]
    deps_task.run()
    lock_data = yaml.safe_load((tmp_path / "package-lock.yml").read_text())
    assert lock_data["packages"] == [{"local": "local_macros", "name": "local_macros"}]
    assert lock_data[PACKAGE_LOCK_HASH_KEY] == _create_sha1_hash(
        deps_task.project.packages.packages
    )
    assert (tmp_path / "dbt_packages" / "local_macros" / "dbt_project.yml").exists()


def test_local_fallback_continues_after_a_local_install_fails(
    deps_task: DepsTask, tmp_path: Path
) -> None:
    write_local_package(tmp_path, "other_local", "other_local")
    deps_task.project.packages.packages.append(LocalPackage(local="other_local"))
    blocked_path = tmp_path / "dbt_packages" / "local_macros"
    blocked_path.parent.mkdir()
    blocked_path.write_text("existing file")
    error = DbtInternalError("remote failed")
    with mock.patch.object(GitPinnedPackage, "_checkout", autospec=True, side_effect=error):
        with pytest.raises(DbtInternalError) as exc:
            deps_task.run()
    assert exc.value is error
    assert blocked_path.read_text() == "existing file"
    assert (tmp_path / "dbt_packages" / "other_local" / "dbt_project.yml").exists()


def test_local_fallback_copies_when_symlinks_are_not_available(
    deps_task: DepsTask, tmp_path: Path
) -> None:
    with (
        mock.patch.object(
            GitPinnedPackage,
            "_checkout",
            autospec=True,
            side_effect=DbtInternalError("remote failed"),
        ),
        mock.patch(
            "dbt.deps.local.system.make_symlink", autospec=True, side_effect=OSError("no symlinks")
        ),
    ):
        with pytest.raises(DbtInternalError):
            deps_task.run()
    installed = tmp_path / "dbt_packages" / "local_macros"
    assert not installed.is_symlink()
    assert (installed / "macros" / "example.sql").exists()


def test_successful_lock_only_does_not_install_packages(
    deps_task: DepsTask, tmp_path: Path
) -> None:
    deps_task.project.packages.packages = [LocalPackage(local="local_macros")]
    deps_task.args.lock = True
    deps_task.run()
    assert (tmp_path / "package-lock.yml").exists()
    assert not (tmp_path / "dbt_packages").exists()
