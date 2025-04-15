import json
from importlib.resources import files
from typing import Any, Dict


def load_json_from_package(jsonschema_type: str, filename: str) -> Dict[str, Any]:
    """Loads a JSON file from within a package."""

    resources = files("dbt")
    path = resources.joinpath("include", "jsonschemas", jsonschema_type, filename)  # type: ignore
    data = path.read_bytes()
    return json.loads(data)


def project_schema() -> Dict[str, Any]:
    return load_json_from_package(jsonschema_type="project", filename="0.0.85.json")
