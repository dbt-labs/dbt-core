from typing import Any, Dict


from dbt.clients import yaml_helper
from dbt.events.functions import fire_event
from dbt.events.types import InvalidOptionYAML
from dbt.exceptions import DbtValidationError, OptionNotYamlDictError


def parse_cli_vars(var_string: str) -> Dict[str, Any]:
    return parse_cli_yaml_string(var_string, "vars")


def parse_cli_yaml_string(var_string: str, cli_option_name: str) -> Dict[str, Any]:
    try:
        cli_vars = yaml_helper.load_yaml_text(var_string)
        var_type = type(cli_vars)
        if var_type is dict:
            return cli_vars
        else:
            raise OptionNotYamlDictError(var_type, cli_option_name)
    except DbtValidationError:
        fire_event(InvalidOptionYAML(option_name=cli_option_name))
        raise
