import os
import sys
from dataclasses import dataclass
from importlib import import_module
from multiprocessing import get_context
from pprint import pformat as pf
from typing import Callable, Dict, List, Set, Union

from click import Context, get_current_context
from click.core import Command, Group, ParameterSource
from dbt.cli.exceptions import DbtUsageException
from dbt.cli.resolvers import default_log_path, default_project_dir
from dbt.config.profile import read_user_config
from dbt.contracts.project import UserConfig
from dbt.deprecations import renamed_env_var
from dbt.helper_types import WarnErrorOptions

if os.name != "nt":
    # https://bugs.python.org/issue41567
    import multiprocessing.popen_spawn_posix  # type: ignore  # noqa: F401

FLAGS_DEFAULTS = {
    "INDIRECT_SELECTION": "eager",
    "TARGET_PATH": None,
    # Cli args without user_config or env var option.
    "FULL_REFRESH": False,
    "STRICT_MODE": False,
    "STORE_FAILURES": False,
    "INTROSPECT": True,
}

DEPRECATED_PARAMS = {
    "deprecated_defer": "defer",
    "deprecated_favor_state": "favor_state",
    "deprecated_print": "print",
    "deprecated_state": "state",
}


def convert_config(config_name, config_value):
    """Convert the values from config and original set_from_args to the correct type."""
    ret = config_value
    if config_name.lower() == "warn_error_options" and type(config_value) == dict:
        ret = WarnErrorOptions(
            include=config_value.get("include", []), exclude=config_value.get("exclude", [])
        )
    return ret


def args_to_context(args: List[str]) -> Context:
    """Convert a list of args to a click context with proper hierarchy for dbt commands"""
    from dbt.cli.main import cli

    cli_ctx = cli.make_context(cli.name, args)
    # Split args if they're a comma seperated string.
    if len(args) == 1 and "," in args[0]:
        args = args[0].split(",")
    sub_command_name, sub_command, args = cli.resolve_command(cli_ctx, args)

    # Handle source and docs group.
    if type(sub_command) == Group:
        sub_command_name, sub_command, args = sub_command.resolve_command(cli_ctx, args)

    assert type(sub_command) == Command
    sub_command_ctx = sub_command.make_context(sub_command_name, args)
    sub_command_ctx.parent = cli_ctx
    return sub_command_ctx


@dataclass(frozen=True)
class Flags:
    """Primary configuration artifact for running dbt"""

    def __init__(self, ctx: Context = None, user_config: UserConfig = None) -> None:

        # Set the default flags.
        for key, value in FLAGS_DEFAULTS.items():
            object.__setattr__(self, key, value)

        if ctx is None:
            ctx = get_current_context()

        def _get_params_by_source(ctx: Context, source_type: ParameterSource):
            """Generates all params of a given source type."""
            yield from [
                name for name, source in ctx._parameter_source.items() if source is source_type
            ]
            if ctx.parent:
                yield from _get_params_by_source(ctx.parent, source_type)

        # Ensure that any params sourced from the commandline are not present more than once.
        # Click handles this exclusivity, but only at a per-subcommand level.
        seen_params = []
        for param in _get_params_by_source(ctx, ParameterSource.COMMANDLINE):
            if param in seen_params:
                raise DbtUsageException(
                    f"{param.lower()} was provided both before and after the subcommand, it can only be set either before or after.",
                )
            seen_params.append(param)

        def _assign_params(
            ctx: Context,
            params_assigned_from_default: set,
            deprecated_env_vars: Dict[str, Callable],
        ):
            """Recursively adds all click params to flag object"""
            for param_name, param_value in ctx.params.items():
                # N.B. You have to use the base MRO method (object.__setattr__) to set attributes
                # when using frozen dataclasses.
                # https://docs.python.org/3/library/dataclasses.html#frozen-instances

                # Handle deprecated env vars while still respecting old values
                # e.g. DBT_NO_PRINT -> DBT_PRINT if DBT_NO_PRINT is set, it is
                # respected over DBT_PRINT or --print.
                new_name: Union[str, None] = None
                if param_name in DEPRECATED_PARAMS:

                    # Deprecated env vars can only be set via env var.
                    # We use the deprecated option in click to serialize the value
                    # from the env var string.
                    param_source = ctx.get_parameter_source(param_name)
                    if param_source == ParameterSource.DEFAULT:
                        continue
                    elif param_source != ParameterSource.ENVIRONMENT:
                        raise DbtUsageException(
                            "Deprecated parameters can only be set via environment variables",
                        )

                    # Rename for clarity.
                    dep_name = param_name
                    new_name = DEPRECATED_PARAMS.get(dep_name)
                    try:
                        assert isinstance(new_name, str)
                    except AssertionError:
                        raise Exception(
                            f"No deprecated param name match in DEPRECATED_PARAMS from {dep_name} to {new_name}"
                        )

                    # Find param objects for their envvar name.
                    try:
                        dep_param = [x for x in ctx.command.params if x.name == dep_name][0]
                        new_param = [x for x in ctx.command.params if x.name == new_name][0]
                    except IndexError:
                        raise Exception(
                            f"No deprecated param name match in context from {dep_name} to {new_name}"
                        )

                    # Remove param from defaulted set since the deprecated
                    # value is not set from default, but from an env var.
                    if new_name in params_assigned_from_default:
                        params_assigned_from_default.remove(new_name)

                    # Add the deprecation warning function to the set.
                    assert isinstance(dep_param.envvar, str)
                    assert isinstance(new_param.envvar, str)
                    deprecated_env_vars[new_name] = renamed_env_var(
                        old_name=dep_param.envvar,
                        new_name=new_param.envvar,
                    )

                # Set the flag value.
                is_duplicate = hasattr(self, param_name.upper())
                is_default = ctx.get_parameter_source(param_name) == ParameterSource.DEFAULT
                flag_name = (new_name or param_name).upper()

                if (is_duplicate and not is_default) or not is_duplicate:
                    object.__setattr__(self, flag_name, param_value)

                # Track default assigned params.
                if is_default:
                    params_assigned_from_default.add(param_name)

            if ctx.parent:
                _assign_params(ctx.parent, params_assigned_from_default, deprecated_env_vars)

        params_assigned_from_default = set()  # type: Set[str]
        deprecated_env_vars: Dict[str, Callable] = {}
        _assign_params(ctx, params_assigned_from_default, deprecated_env_vars)

        # Set deprecated_env_var_warnings to be fired later after events have been init.
        object.__setattr__(
            self, "deprecated_env_var_warnings", [x for x in deprecated_env_vars.values()]
        )

        # Get the invoked command flags.
        invoked_subcommand_name = (
            ctx.invoked_subcommand if hasattr(ctx, "invoked_subcommand") else None
        )
        if invoked_subcommand_name is not None:
            invoked_subcommand = getattr(import_module("dbt.cli.main"), invoked_subcommand_name)
            invoked_subcommand.allow_extra_args = True
            invoked_subcommand.ignore_unknown_options = True
            invoked_subcommand_ctx = invoked_subcommand.make_context(None, sys.argv)
            _assign_params(
                invoked_subcommand_ctx, params_assigned_from_default, deprecated_env_vars
            )

        if not user_config:
            profiles_dir = getattr(self, "PROFILES_DIR", None)
            user_config = read_user_config(profiles_dir) if profiles_dir else None

        # Overwrite default assignments with user config if available.
        if user_config:
            param_assigned_from_default_copy = params_assigned_from_default.copy()
            for param_assigned_from_default in params_assigned_from_default:
                user_config_param_value = getattr(user_config, param_assigned_from_default, None)
                if user_config_param_value is not None:
                    object.__setattr__(
                        self,
                        param_assigned_from_default.upper(),
                        convert_config(param_assigned_from_default, user_config_param_value),
                    )
                    param_assigned_from_default_copy.remove(param_assigned_from_default)
            params_assigned_from_default = param_assigned_from_default_copy

        # Set hard coded flags.
        object.__setattr__(self, "WHICH", invoked_subcommand_name or ctx.info_name)
        object.__setattr__(self, "MP_CONTEXT", get_context("spawn"))

        # Apply the lead/follow relationship between some parameters.
        self._override_if_set("USE_COLORS", "USE_COLORS_FILE", params_assigned_from_default)
        self._override_if_set("LOG_LEVEL", "LOG_LEVEL_FILE", params_assigned_from_default)
        self._override_if_set("LOG_FORMAT", "LOG_FORMAT_FILE", params_assigned_from_default)

        # Set default LOG_PATH from PROJECT_DIR, if available.
        # Starting in v1.5, if `log-path` is set in `dbt_project.yml`, it will raise a deprecation warning,
        # with the possibility of removing it in a future release.
        if getattr(self, "LOG_PATH", None) is None:
            project_dir = getattr(self, "PROJECT_DIR", default_project_dir())
            version_check = getattr(self, "VERSION_CHECK", True)
            object.__setattr__(self, "LOG_PATH", default_log_path(project_dir, version_check))

        # Support console DO NOT TRACK initiative.
        if os.getenv("DO_NOT_TRACK", "").lower() in ("1", "t", "true", "y", "yes"):
            object.__setattr__(self, "SEND_ANONYMOUS_USAGE_STATS", False)

        # Check mutual exclusivity once all flags are set.
        self._assert_mutually_exclusive(
            params_assigned_from_default, ["WARN_ERROR", "WARN_ERROR_OPTIONS"]
        )

        # Support lower cased access for legacy code.
        params = set(
            x for x in dir(self) if not callable(getattr(self, x)) and not x.startswith("__")
        )
        for param in params:
            object.__setattr__(self, param.lower(), getattr(self, param))

    def __str__(self) -> str:
        return str(pf(self.__dict__))

    def _override_if_set(self, lead: str, follow: str, defaulted: Set[str]) -> None:
        """If the value of the lead parameter was set explicitly, apply the value to follow, unless follow was also set explicitly."""
        if lead.lower() not in defaulted and follow.lower() in defaulted:
            object.__setattr__(self, follow.upper(), getattr(self, lead.upper(), None))

    def _assert_mutually_exclusive(
        self, params_assigned_from_default: Set[str], group: List[str]
    ) -> None:
        """
        Ensure no elements from group are simultaneously provided by a user, as inferred from params_assigned_from_default.
        Raises click.UsageError if any two elements from group are simultaneously provided by a user.
        """
        set_flag = None
        for flag in group:
            flag_set_by_user = flag.lower() not in params_assigned_from_default
            if flag_set_by_user and set_flag:
                raise DbtUsageException(
                    f"{flag.lower()}: not allowed with argument {set_flag.lower()}"
                )
            elif flag_set_by_user:
                set_flag = flag

    def fire_deprecations(self):
        """Fires events for deprecated env_var usage."""
        [dep_fn() for dep_fn in self.deprecated_env_var_warnings]
        # It is necessary to remove this attr from the class so it does
        # not get pickled when written to disk as json.
        object.__delattr__(self, "deprecated_env_var_warnings")
