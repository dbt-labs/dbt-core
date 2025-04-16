import json
from pathlib import Path
from typing import Any, Dict

from dbt.include.jsonschemas import JSONSCHEMAS_PATH


def load_json_from_package(jsonschema_type: str, filename: str) -> Dict[str, Any]:
    """Loads a JSON file from within a package."""

    path = Path(JSONSCHEMAS_PATH).joinpath(jsonschema_type, filename)
    data = path.read_bytes()
    return json.loads(data)


def project_schema() -> Dict[str, Any]:
    return load_json_from_package(jsonschema_type="project", filename="0.0.85.json")


def resources_schema() -> Dict[str, Any]:
    return load_json_from_package(jsonschema_type="resources", filename="0.0.85.json")
