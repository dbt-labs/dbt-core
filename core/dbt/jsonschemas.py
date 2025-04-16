import json
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict

from jsonschema._keywords import type as type_rule
from jsonschema.validators import Draft7Validator, extend

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


def custom_type_rule(validator, types, instance, schema):
    """This is necessary because PyYAML loads things that look like dates or datetimes as those
    python objects. Then jsonschema.validate() fails because it expects strings.
    """
    if "string" in types and (isinstance(instance, datetime) or isinstance(instance, date)):
        return
    else:
        return type_rule(validator, types, instance, schema)


CustomDraft7Validator = extend(Draft7Validator, validators={"type": custom_type_rule})
