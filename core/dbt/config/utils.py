from typing import Any, Dict


from dbt.clients import yaml_helper
from dbt_common.events.functions import fire_event
from dbt.events.types import InvalidOptionYAML
from dbt.exceptions import OptionNotYamlDictError
from dbt_common.exceptions import DbtConfigError, DbtValidationError


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
    except (DbtValidationError, OptionNotYamlDictError):
        fire_event(InvalidOptionYAML(option_name=cli_option_name))
        raise


def exclusive_primary_alt_value_setting(
    dictionary: Dict[str, Any], primary: str, alt: str
) -> None:
    """Munges in place under the primary the options for the primary and alt values

    Sometimes we allow setting something via TWO keys, but not at the same time. If both the primary
    key and alt key have values, an error gets raised. If the alt key has values, then we update
    the dictionary to ensure the primary key contains the values. If neither are set, nothing happens.
    """

    primary_options = dictionary.get(primary)
    alt_options = dictionary.get(alt)

    if primary_options and alt_options:
        raise DbtConfigError(
            f"Only `{alt}` or `{primary}` can be specified in `warn_error_options`, not both"
        )

    if alt_options:
        dictionary[primary] = alt_options
