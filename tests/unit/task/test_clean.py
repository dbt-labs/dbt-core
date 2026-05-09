"""Unit tests for CleanTask.

Regression coverage for dbt-labs/dbt-core#11346: ``dbt clean`` must honour
``--target-path`` / ``DBT_TARGET_PATH`` overrides even when the user has
explicitly defined ``clean-targets`` in ``dbt_project.yml``.
"""
from pathlib import Path
from unittest.mock import MagicMock, patch

from dbt.task.clean import CleanTask


def _make_task(tmp_path: Path, *, clean_targets, target_path):
    """Build a CleanTask wired up with mocks for the bits we don't exercise."""
    args = MagicMock()
    args.project_dir = str(tmp_path)
    args.clean_project_files_only = False

    project = MagicMock()
    project.clean_targets = list(clean_targets)
    project.target_path = target_path
    project.all_source_paths = []
    project.test_paths = []
    project.packages_install_path = "dbt_packages"

    task = CleanTask(args=args, config=project)
    return task


@patch("dbt.task.clean.move_to_nearest_project_dir")
@patch("dbt.task.clean.fire_event")
@patch("dbt.task.clean.rmtree")
def test_clean_includes_overridden_target_path(
    mock_rmtree, _mock_fire, mock_move, tmp_path
):
    """Even when ``clean-targets`` does NOT mention the overridden path,
    CleanTask must still attempt to remove the effective target_path.
    """
    mock_move.return_value = tmp_path

    custom_target = tmp_path / "my_custom_target"
    custom_target.mkdir()

    task = _make_task(
        tmp_path,
        clean_targets=[str(tmp_path / "target")],  # default target only
        target_path=str(custom_target),  # flag override
    )

    task.run()

    cleaned = {Path(c.args[0]).resolve() for c in mock_rmtree.call_args_list}
    assert custom_target.resolve() in cleaned, (
        "Expected the directory pointed to by --target-path to be cleaned, but "
        f"only saw rmtree calls for: {cleaned}"
    )


@patch("dbt.task.clean.move_to_nearest_project_dir")
@patch("dbt.task.clean.fire_event")
@patch("dbt.task.clean.rmtree")
def test_clean_still_includes_clean_targets(
    mock_rmtree, _mock_fire, mock_move, tmp_path
):
    """Adding the effective target_path must not displace existing clean_targets."""
    mock_move.return_value = tmp_path

    pkgs = tmp_path / "dbt_packages"
    pkgs.mkdir()
    target = tmp_path / "target"
    target.mkdir()

    task = _make_task(
        tmp_path,
        clean_targets=[str(pkgs), str(target)],
        target_path=str(target),
    )

    task.run()

    cleaned = {Path(c.args[0]).resolve() for c in mock_rmtree.call_args_list}
    assert pkgs.resolve() in cleaned
    assert target.resolve() in cleaned
