import json
import re
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, Iterator

import jsonschema
from jsonschema import ValidationError
from jsonschema._keywords import type as type_rule
from jsonschema.validators import Draft7Validator, extend

from dbt import deprecations
from dbt.include.jsonschemas import JSONSCHEMAS_PATH


def load_json_from_package(jsonschema_type: str, filename: str) -> Dict[str, Any]:
    """Loads a JSON file from within a package."""

    path = Path(JSONSCHEMAS_PATH).joinpath(jsonschema_type, filename)
    data = path.read_bytes()
    return json.loads(data)


def project_schema() -> Dict[str, Any]:
    return load_json_from_package(jsonschema_type="project", filename="0.0.110.json")


def resources_schema() -> Dict[str, Any]:
    return load_json_from_package(jsonschema_type="resources", filename="0.0.110.json")


def custom_type_rule(validator, types, instance, schema):
    """This is necessary because PyYAML loads things that look like dates or datetimes as those
    python objects. Then jsonschema.validate() fails because it expects strings.
    """
    if "string" in types and (isinstance(instance, datetime) or isinstance(instance, date)):
        return
    else:
        return type_rule(validator, types, instance, schema)


CustomDraft7Validator = extend(Draft7Validator, validators={"type": custom_type_rule})


def error_path_to_string(error: jsonschema.ValidationError) -> str:
    if len(error.path) == 0:
        return ""
    else:
        path = str(error.path.popleft())
        for part in error.path:
            if isinstance(part, int):
                path += f"[{part}]"
            else:
                path += f".{part}"

        return path


def jsonschema_validate(schema: Dict[str, Any], json: Dict[str, Any], file_path: str) -> None:
    validator = CustomDraft7Validator(schema)
    errors: Iterator[ValidationError] = validator.iter_errors(json)  # get all validation errors

    for error in errors:
        # Listify the error path to make it easier to work with (it's a deque in the ValidationError object)
        error_path = list(error.path)
        if error.validator == "additionalProperties":
            key = re.search(r"'\S+'", error.message)
            if len(error.path) == 0:
                deprecations.warn(
                    "custom-top-level-key-deprecation",
                    msg="Unexpected top-level key" + (" " + key.group() if key else ""),
                    file=file_path,
                )
            else:
                deprecations.warn(
                    "custom-key-in-object-deprecation",
                    key=key.group() if key else "",
                    file=file_path,
                    key_path=error_path_to_string(error),
                )
        elif (
            error.validator == "anyOf"
            and len(error_path) > 0
            and error_path[-1] == "config"
            and isinstance(error.instance, dict)
            and len(error.instance.keys()) > 0
        ):
            deprecations.warn(
                "custom-key-in-config-deprecation",
                key=(list(error.instance.keys()))[0],
                file=file_path,
                key_path=error_path_to_string(error),
            )
        else:
            deprecations.warn(
                "generic-json-schema-validation-deprecation",
                violation=error.message,
                file=file_path,
                key_path=error_path_to_string(error),
            )
