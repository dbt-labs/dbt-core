from typing import Dict, Any

from dbt.clients import yaml_helper
from dbt.events.functions import fire_event
from dbt.exceptions import raise_compiler_error, ValidationException
from dbt.events.types import InvalidVarsYAML


def parse_cli_vars(var_string: str) -> Dict[str, Any]:
    try:
        cli_vars = yaml_helper.load_yaml_text(var_string)
        var_type = type(cli_vars)
        if var_type is dict:
            return cli_vars
        else:
            type_name = var_type.__name__
            raise_compiler_error(
                "The --vars argument must be a YAML dictionary, but was "
                "of type '{}'".format(type_name)
            )
    except ValidationException:
        fire_event(InvalidVarsYAML())
        raise
