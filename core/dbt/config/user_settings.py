from __future__ import annotations

from pathlib import Path

import yaml

from dbt.clients.yaml_helper import load_yaml_text
from dbt.constants import USER_SETTINGS_FILE_NAME
from dbt.contracts.user_settings import UserSettings
from dbt_common.clients.system import load_file_contents
from dbt_common.dataclass_schema import ValidationError
from dbt_common.exceptions import DbtValidationError


def _default_path() -> Path:
    from dbt.cli.resolvers import default_dbt_home_dir

    return default_dbt_home_dir() / USER_SETTINGS_FILE_NAME


def _load_yaml_mapping(path: Path) -> dict | None:
    if not path.is_file():
        return None

    try:
        contents = load_file_contents(str(path), strip=False)
    except OSError as e:
        raise DbtValidationError(f"cannot read {path}: {e}") from e

    parsed = load_yaml_text(contents)
    if parsed is None:
        return None
    if not isinstance(parsed, dict):
        raise DbtValidationError(f"expected mapping in {path}, got {type(parsed).__name__}")
    return parsed


def read_user_settings(path: Path | None = None) -> UserSettings:
    if path is None:
        path = _default_path()
    parsed = _load_yaml_mapping(path)
    if parsed is None:
        return UserSettings()
    try:
        return UserSettings.from_dict(parsed)
    except ValidationError as e:
        raise DbtValidationError(f"invalid user settings in {path}: {e}") from e


def write_user_settings(settings: UserSettings, path: Path | None = None) -> None:
    if path is None:
        path = _default_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(yaml.dump(settings.to_dict(), default_flow_style=False), encoding="utf-8")


def get_user_setting_flags(path: Path | None = None) -> dict:
    try:
        settings = read_user_settings(path)
    except (DbtValidationError, RuntimeError):
        # RuntimeError: Path.home() fails when HOME/USERPROFILE env vars are absent.
        return {}
    return settings.flags


def set_user_setting_flag(name: str, value: bool, path: Path | None = None) -> None:
    settings = read_user_settings(path)
    settings.flags[name] = value
    write_user_settings(settings, path)
