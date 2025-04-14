import importlib
import json
from typing import Any, Dict

from dbt.cli.exceptions import DbtInternalException


def load_json_from_package(package_name, filename) -> Dict[str, Any]:
    """Loads a JSON file from within a package."""
    try:
        # TODO: fix this type error
        with importlib.resources.open_text(package_name, filename) as file:  # type: ignore
            return json.load(file)
    except FileNotFoundError:
        raise DbtInternalException(f"File `{filename}` not found in package `{package_name}`")
    except json.JSONDecodeError:
        raise DbtInternalException(
            f"Invalid JSON format in {filename} within package {package_name}"
        )


def load_project_schema() -> Dict[str, Any]:
    package_name = "dbt.resources.input_schemas.project"
    filename = "0.0.85.json"
    return load_json_from_package(package_name, filename)
