from click import ParamType, Choice

from dbt.config.utils import parse_cli_yaml_string
from dbt.events import ALL_EVENT_NAMES
from dbt.exceptions import ValidationError, OptionNotYamlDictError
from dbt_common.exceptions import DbtConfigError, DbtValidationError
from dbt_common.helper_types import WarnErrorOptions
from typing import Any, Dict


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


class Package(ParamType):
    """The Click STRING type. Converts string into dict with package name and version.
    Example package:
        package-name@1.0.0
        package-name
    """

    name = "NewPackage"

    def convert(self, value, param, ctx):
        # assume non-string values are a problem
        if not isinstance(value, str):
            self.fail(f"Cannot load Package from type {type(value)}", param, ctx)
        try:
            package_name, package_version = value.split("@")
            return {"name": package_name, "version": package_version}
        except ValueError:
            return {"name": value, "version": None}


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


class WarnErrorOptionsType(YAML):
    """The Click WarnErrorOptions type. Converts YAML strings into objects."""

    name = "WarnErrorOptionsType"

    def convert(self, value, param, ctx):
        # this function is being used by param in click
        include_exclude = super().convert(value, param, ctx)
        exclusive_primary_alt_value_setting(include_exclude, "include", "error")
        exclusive_primary_alt_value_setting(include_exclude, "exclude", "warn")

        return WarnErrorOptions(
            include=include_exclude.get("include", []),
            exclude=include_exclude.get("exclude", []),
            silence=include_exclude.get("silence", []),
            valid_error_names=ALL_EVENT_NAMES,
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
        if not isinstance(value, str):
            for value_item in value:
                super().convert(value_item, param, ctx)
        else:
            super().convert(value, param, ctx)

        return value
