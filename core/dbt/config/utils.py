from typing import Any, Dict, Optional, Set

from dbt import deprecations
from dbt.clients import yaml_helper
from dbt.events.fusion_warn_error_options import FUSION_WARN_ERROR_OPTION_NAMES
from dbt.events.types import InvalidOptionYAML
from dbt.exceptions import DbtExclusivePropertyUseError, OptionNotYamlDictError
from dbt_common.events.functions import fire_event
from dbt_common.events.types import Note
from dbt_common.exceptions import DbtValidationError
from dbt_common.helper_types import WarnErrorOptionsV2


def parse_cli_vars(var_string: str) -> Dict[str, Any]:
    return parse_cli_yaml_string(var_string, "vars")


def parse_cli_yaml_string(var_string: str, cli_option_name: str) -> Dict[str, Any]:
    try:
        cli_vars = yaml_helper.load_yaml_text(var_string)
        var_type = type(cli_vars)
        if cli_vars is not None and var_type is dict:
            return cli_vars
        else:
            raise OptionNotYamlDictError(var_type, cli_option_name)
    except (DbtValidationError, OptionNotYamlDictError):
        fire_event(InvalidOptionYAML(option_name=cli_option_name))
        raise


def exclusive_primary_alt_value_setting(
    dictionary: Optional[Dict[str, Any]],
    primary: str,
    alt: str,
    parent_config: Optional[str] = None,
) -> None:
    """Munges in place under the primary the options for the primary and alt values

    Sometimes we allow setting something via TWO keys, but not at the same time. If both the primary
    key and alt key have values, an error gets raised. If the alt key has values, then we update
    the dictionary to ensure the primary key contains the values. If neither are set, nothing happens.
    """

    if dictionary is None:
        return

    primary_options = dictionary.get(primary)
    alt_options = dictionary.get(alt)

    if primary_options and alt_options:
        where = f" in `{parent_config}`" if parent_config is not None else ""
        raise DbtExclusivePropertyUseError(
            f"Only `{alt}` or `{primary}` can be specified{where}, not both"
        )

    if alt in dictionary:
        alt_value = dictionary.pop(alt)
        dictionary[primary] = alt_value


def normalize_warn_error_options(warn_error_options: Dict[str, Any]) -> None:
    has_include = "include" in warn_error_options
    has_exclude = "exclude" in warn_error_options

    if has_include or has_exclude:
        deprecations.buffer(
            "weo-include-exclude-deprecation",
            found_include=has_include,
            found_exclude=has_exclude,
        )

    exclusive_primary_alt_value_setting(
        warn_error_options, "error", "include", "warn_error_options"
    )
    exclusive_primary_alt_value_setting(
        warn_error_options, "warn", "exclude", "warn_error_options"
    )
    for key in ("error", "warn", "silence"):
        if key in warn_error_options and warn_error_options[key] is None:
            warn_error_options[key] = []


def extract_fusion_only_warn_error_options(
    warn_error_options: Dict[str, Any], valid_error_names: Set[str]
) -> Set[str]:
    """Remove dbt Fusion-only warning names from error/warn/silence, in place.

    A name is considered Fusion-only when it is recognized by the dbt Fusion
    engine (``FUSION_WARN_ERROR_OPTION_NAMES``) but is *not* a valid dbt-core
    event name. Such names are meaningless to dbt-core, so we strip them here
    -- rather than letting ``WarnErrorOptionsV2`` reject them -- which lets a
    project share ``warn_error_options`` between Fusion and Core. Names that are
    valid dbt-core events (including the few that coincide with Fusion error
    codes) are left untouched, and genuinely unknown names are left in place so
    ``WarnErrorOptionsV2`` still raises on typos.

    Returns the set of Fusion-only names that were removed.
    """
    removed: Set[str] = set()
    for key in ("error", "warn", "silence"):
        value = warn_error_options.get(key)
        if not isinstance(value, list):
            continue
        kept = []
        for name in value:
            if (
                isinstance(name, str)
                and name in FUSION_WARN_ERROR_OPTION_NAMES
                and name not in valid_error_names
            ):
                removed.add(name)
            else:
                kept.append(name)
        warn_error_options[key] = kept
    return removed


def build_warn_error_options_v2(
    warn_error_options: Dict[str, Any], valid_error_names: Set[str]
) -> WarnErrorOptionsV2:
    """Build a ``WarnErrorOptionsV2``, tolerating dbt Fusion-only warning names.

    Fusion-only names are stripped and reported with a note (rather than raising)
    so that configuration shared with the dbt Fusion engine still runs under
    dbt-core. ``warn_error_options`` is expected to have already been passed
    through ``normalize_warn_error_options``.
    """
    for name in sorted(
        extract_fusion_only_warn_error_options(warn_error_options, valid_error_names)
    ):
        fire_event(
            Note(msg=f"{name} is not being used because it's specific to the dbt Fusion engine.")
        )

    return WarnErrorOptionsV2(
        error=warn_error_options.get("error", []),
        warn=warn_error_options.get("warn", []),
        silence=warn_error_options.get("silence", []),
        valid_error_names=valid_error_names,
    )
