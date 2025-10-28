import json
import os
import re
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional, Union

import jsonschema
from jsonschema import ValidationError
from jsonschema._keywords import type as type_rule
from jsonschema.validators import Draft7Validator, extend

from dbt import deprecations
from dbt.jsonschemas import JSONSCHEMAS_PATH
from dbt_common.context import get_invocation_context

_PROJECT_SCHEMA: Optional[Dict[str, Any]] = None
_RESOURCES_SCHEMA: Optional[Dict[str, Any]] = None

_JSONSCHEMA_SUPPORTED_ADAPTERS = {
    "bigquery",
    "databricks",
    "redshift",
    "snowflake",
}

_HIERARCHICAL_CONFIG_KEYS = {
    "seeds",
    "sources",
    "models",
    "snapshots",
    "tests",
    "exposures",
    "data_tests",
    "metrics",
    "saved_queries",
    "semantic_models",
    "unit_tests",
}


def load_json_from_package(jsonschema_type: str, filename: str) -> Dict[str, Any]:
    """Loads a JSON file from within a package."""

    path = Path(JSONSCHEMAS_PATH).joinpath(jsonschema_type, filename)
    data = path.read_bytes()
    return json.loads(data)


def project_schema() -> Dict[str, Any]:
    global _PROJECT_SCHEMA

    if _PROJECT_SCHEMA is None:
        _PROJECT_SCHEMA = load_json_from_package(
            jsonschema_type="project", filename="0.0.110.json"
        )
    return _PROJECT_SCHEMA


def resources_schema() -> Dict[str, Any]:
    global _RESOURCES_SCHEMA

    if _RESOURCES_SCHEMA is None:
        _RESOURCES_SCHEMA = load_json_from_package(
            jsonschema_type="resources", filename="latest.json"
        )

    return _RESOURCES_SCHEMA


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


def _additional_properties_violation_keys(error: ValidationError) -> List[str]:
    found_keys = re.findall(r"'\S+'", error.message)
    return [key.strip("'") for key in found_keys]


def _validate_with_schema(
    schema: Dict[str, Any], json: Dict[str, Any]
) -> Iterator[ValidationError]:
    validator = CustomDraft7Validator(schema)
    return validator.iter_errors(json)


def _get_allowed_config_fields_from_error_path(
    yml_schema: Dict[str, Any], error_path: List[Union[str, int]]
) -> Optional[List[str]]:
    property_field_name = None
    node_schema = yml_schema["properties"]
    for part in error_path:
        if isinstance(part, str):
            if part in node_schema:
                if "items" not in node_schema[part]:
                    break

                # Update property field name
                property_field_name = node_schema[part]["items"]["$ref"].split("/")[-1]

                # Jump to the next level of the schema
                item_definition = node_schema[part]["items"]["$ref"].split("/")[-1]
                node_schema = yml_schema["definitions"][item_definition]["properties"]

    if not property_field_name:
        return None

    if "config" not in yml_schema["definitions"][property_field_name]["properties"]:
        return None

    config_field_name = yml_schema["definitions"][property_field_name]["properties"]["config"][
        "anyOf"
    ][0]["$ref"].split("/")[-1]

    allowed_config_fields = list(set(yml_schema["definitions"][config_field_name]["properties"]))

    return allowed_config_fields


def _can_run_validations() -> bool:
    if not os.environ.get("DBT_ENV_PRIVATE_RUN_JSONSCHEMA_VALIDATIONS"):
        return False

    invocation_context = get_invocation_context()
    return invocation_context.adapter_types.issubset(_JSONSCHEMA_SUPPORTED_ADAPTERS)


def jsonschema_validate(schema: Dict[str, Any], json: Dict[str, Any], file_path: str) -> None:
    if not _can_run_validations():
        return

    errors = _validate_with_schema(schema, json)
    for error in errors:
        # Listify the error path to make it easier to work with (it's a deque in the ValidationError object)
        error_path = list(error.path)
        if error.validator == "additionalProperties":
            keys = _additional_properties_violation_keys(error)
            if len(error.path) == 0:
                for key in keys:
                    deprecations.warn(
                        "custom-top-level-key-deprecation",
                        msg="Unexpected top-level key" + (" " + key if key else ""),
                        file=file_path,
                    )
            else:
                key_path = error_path_to_string(error)
                for key in keys:
                    if key == "overrides" and key_path.startswith("sources"):

                        deprecations.warn(
                            "source-override-deprecation",
                            source_name=key_path.split(".")[-1],
                            file=file_path,
                        )
                    else:
                        allowed_config_fields = _get_allowed_config_fields_from_error_path(
                            schema, error_path
                        )
                        if allowed_config_fields and key in allowed_config_fields:
                            deprecations.warn(
                                "property-moved-to-config-deprecation",
                                key=key,
                                file=file_path,
                                key_path=key_path,
                            )
                        else:
                            deprecations.warn(
                                "custom-key-in-object-deprecation",
                                key=key,
                                file=file_path,
                                key_path=key_path,
                            )
        elif error.validator == "anyOf" and len(error_path) > 0:
            sub_errors = error.context or []
            # schema yaml resource configs
            if error_path[-1] == "config":
                for sub_error in sub_errors:
                    if (
                        isinstance(sub_error, ValidationError)
                        and sub_error.validator == "additionalProperties"
                    ):
                        keys = _additional_properties_violation_keys(sub_error)
                        key_path = error_path_to_string(error)
                        for key in keys:
                            deprecations.warn(
                                "custom-key-in-config-deprecation",
                                key=key,
                                file=file_path,
                                key_path=key_path,
                            )
            # dbt_project.yml configs
            elif "dbt_project.yml" in file_path and error_path[0] in _HIERARCHICAL_CONFIG_KEYS:
                for sub_error in sub_errors:
                    if isinstance(sub_error, ValidationError) and sub_error.validator == "type":
                        # Only raise type-errors if they are indicating leaf config without a plus prefix
                        if (
                            len(sub_error.path) > 0
                            and isinstance(sub_error.path[-1], str)
                            and not sub_error.path[-1].startswith("+")
                        ):
                            deprecations.warn(
                                "missing-plus-prefix-in-config-deprecation",
                                key=sub_error.path[-1],
                                file=file_path,
                                key_path=error_path_to_string(sub_error),
                            )
        elif error.validator == "type":
            # Not deprecating invalid types yet
            pass
        else:
            deprecations.warn(
                "generic-json-schema-validation-deprecation",
                violation=error.message,
                file=file_path,
                key_path=error_path_to_string(error),
            )


def validate_model_config(config: Dict[str, Any], file_path: str) -> None:
    if not _can_run_validations():
        return

    resources_jsonschema = resources_schema()
    nested_definition_name = "ModelConfig"

    model_config_schema = {
        "$schema": "http://json-schema.org/draft-07/schema#",
        "title": nested_definition_name,
        **resources_jsonschema["definitions"][nested_definition_name],
        "definitions": {
            k: v
            for k, v in resources_jsonschema["definitions"].items()
            if k != nested_definition_name
        },
    }

    errors = _validate_with_schema(model_config_schema, config)
    for error in errors:
        error_path = list(error.path)
        if error.validator == "additionalProperties":
            keys = _additional_properties_violation_keys(error)
            if len(error.path) == 0:
                key_path = error_path_to_string(error)
                for key in keys:
                    deprecations.warn(
                        "custom-key-in-config-deprecation",
                        key=key,
                        file=file_path,
                        key_path=key_path,
                    )
            else:
                error.path.appendleft("config")
                key_path = error_path_to_string(error)
                for key in keys:
                    deprecations.warn(
                        "custom-key-in-object-deprecation",
                        key=key,
                        file=file_path,
                        key_path=key_path,
                    )
        elif error.validator == "type":
            # Not deprecating invalid types yet, except for pre-existing deprecation_date deprecation
            pass
        elif error.validator == "anyOf" and len(error_path) > 0:
            for sub_error in error.context or []:
                if (
                    isinstance(sub_error, ValidationError)
                    and sub_error.validator == "additionalProperties"
                ):
                    error.path.appendleft("config")
                    keys = _additional_properties_violation_keys(sub_error)
                    key_path = error_path_to_string(error)
                    for key in keys:
                        deprecations.warn(
                            "custom-key-in-object-deprecation",
                            key=key,
                            file=file_path,
                            key_path=key_path,
                        )
        else:
            deprecations.warn(
                "generic-json-schema-validation-deprecation",
                violation=error.message,
                file=file_path,
                key_path=error_path_to_string(error),
            )
