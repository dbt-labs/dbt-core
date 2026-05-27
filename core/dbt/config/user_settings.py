from __future__ import annotations

from pathlib import Path

import yaml

from dbt_common.exceptions import DbtValidationError


def _resolve_path(path: Path | None) -> Path:
    if path is not None:
        return path
    from dbt.cli.resolvers import default_dbt_home_dir

    return default_dbt_home_dir() / "user_settings.yml"


def read_user_settings(path: Path | None = None) -> dict:
    path = _resolve_path(path)
    try:
        content = path.read_text(encoding="utf-8")
    except FileNotFoundError:
        return {}
    except OSError as e:
        raise DbtValidationError(f"cannot read {path}: {e}") from e

    try:
        parsed = yaml.safe_load(content)
    except yaml.YAMLError as e:
        raise DbtValidationError(f"invalid YAML in {path}: {e}") from e

    if parsed is None:
        return {}
    if not isinstance(parsed, dict):
        raise DbtValidationError(f"expected mapping in {path}, got {type(parsed).__name__}")

    return parsed


def write_user_settings(settings: dict, path: Path | None = None) -> None:
    path = _resolve_path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(yaml.dump(settings, default_flow_style=False), encoding="utf-8")


def get_user_setting_flags(path: Path | None = None) -> dict:
    try:
        settings = read_user_settings(path)
    except DbtValidationError:
        return {}
    flags = settings.get("flags", {})
    if not isinstance(flags, dict):
        return {}
    return flags


def set_user_setting_flag(name: str, value: bool, path: Path | None = None) -> None:
    settings = read_user_settings(path)
    if "flags" not in settings or not isinstance(settings.get("flags"), dict):
        settings["flags"] = {}
    settings["flags"][name] = value
    write_user_settings(settings, path)
