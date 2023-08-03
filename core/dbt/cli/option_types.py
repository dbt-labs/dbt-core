from click import ParamType, Choice

from dbt.config.utils import parse_cli_yaml_string
from dbt.exceptions import ValidationError, DbtValidationError, OptionNotYamlDictError

from dbt.helper_types import WarnErrorOptions


class YAML(ParamType):
    """The Click YAML type. Converts YAML strings into objects."""

    name = "YAML"

    def convert(self, value, param, ctx):
        # assume non-string values are a problem
        if not isinstance(value, str):
            self.fail(f"Cannot load YAML from type {type(value)}", param, ctx)
        try:
            param_option_name = param.opts[0] if param.opts else param.name
            return parse_cli_yaml_string(value, param_option_name.strip("-"))
        except (ValidationError, DbtValidationError, OptionNotYamlDictError):
            self.fail(f"String '{value}' is not valid YAML", param, ctx)


class WarnErrorOptionsType(YAML):
    """The Click WarnErrorOptions type. Converts YAML strings into objects."""

    name = "WarnErrorOptionsType"

    def convert(self, value, param, ctx):
        # this function is being used by param in click
        include_exclude = super().convert(value, param, ctx)

        return WarnErrorOptions(
            include=include_exclude.get("include", []), exclude=include_exclude.get("exclude", [])
        )


class Truthy(ParamType):
    """The Click Truthy type.  Converts strings into a "truthy" type"""

    name = "TRUTHY"

    def convert(self, value, param, ctx):
        # assume non-string / non-None values are a problem
        if not isinstance(value, (str, None)):
            self.fail(f"Cannot load TRUTHY from type {type(value)}", param, ctx)

        if value is None or value.lower() in ("0", "false", "f"):
            return None
        else:
            return value


class ChoiceTuple(Choice):
    name = "CHOICE_TUPLE"

    def convert(self, value, param, ctx):
        for value_item in value:
            super().convert(value_item, param, ctx)

        return value
