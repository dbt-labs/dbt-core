# TODO  Move this to /core/dbt/flags.py when we're ready to break things
import os
import sys
from dataclasses import dataclass
from importlib import import_module
from multiprocessing import get_context
from pprint import pformat as pf
from typing import Set, List

from click import Context, get_current_context, BadOptionUsage
from click.core import ParameterSource, Command, Group

from dbt.config.profile import read_user_config
from dbt.contracts.project import UserConfig
from dbt.helper_types import WarnErrorOptions
from dbt.cli.resolvers import default_project_dir, default_log_path


if os.name != "nt":
    # https://bugs.python.org/issue41567
    import multiprocessing.popen_spawn_posix  # type: ignore  # noqa: F401

# TODO anything that has a default in params should be removed here?
# Or maybe only the ones that's in the root click group
FLAGS_DEFAULTS = {
    "INDIRECT_SELECTION": "eager",
    "TARGET_PATH": None,
    # cli args without user_config or env var option
    "FULL_REFRESH": False,
    "STRICT_MODE": False,
    "STORE_FAILURES": False,
}


# For backwards compatability, some params are defined across multiple levels,
# Top-level value should take precedence.
# e.g. dbt --target-path test2 run --target-path test2
EXPECTED_DUPLICATE_PARAMS = [
    "full_refresh",
    "target_path",
    "version_check",
    "fail_fast",
    "indirect_selection",
    "store_failures",
]


def convert_config(config_name, config_value):
    # This function should take care of converting the values from config and original
    # set_from_args to the correct type
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
    # args would get converted during make context
    if len(args) == 1 and "," in args[0]:
        args = args[0].split(",")
    sub_command_name, sub_command, args = cli.resolve_command(cli_ctx, args)

    # handle source and docs group
    if type(sub_command) == Group:
        sub_command_name, sub_command, args = sub_command.resolve_command(cli_ctx, args)

    assert type(sub_command) == Command
    sub_command_ctx = sub_command.make_context(sub_command_name, args)
    sub_command_ctx.parent = cli_ctx
    return sub_command_ctx


@dataclass(frozen=True)
class Flags:
    def __init__(self, ctx: Context = None, user_config: UserConfig = None) -> None:

        # set the default flags
        for key, value in FLAGS_DEFAULTS.items():
            object.__setattr__(self, key, value)

        if ctx is None:
            ctx = get_current_context()

        def assign_params(ctx, params_assigned_from_default):
            """Recursively adds all click params to flag object"""
            for param_name, param_value in ctx.params.items():
                # TODO: this is to avoid duplicate params being defined in two places (version_check in run and cli)
                # However this is a bit of a hack and we should find a better way to do this

                # N.B. You have to use the base MRO method (object.__setattr__) to set attributes
                # when using frozen dataclasses.
                # https://docs.python.org/3/library/dataclasses.html#frozen-instances
                if hasattr(self, param_name.upper()):
                    if param_name not in EXPECTED_DUPLICATE_PARAMS:
                        raise Exception(
                            f"Duplicate flag names found in click command: {param_name}"
                        )
                    else:
                        # Expected duplicate param from multi-level click command (ex: dbt --full_refresh run --full_refresh)
                        # Overwrite user-configured param with value from parent context
                        if ctx.get_parameter_source(param_name) != ParameterSource.DEFAULT:
                            object.__setattr__(self, param_name.upper(), param_value)
                else:
                    object.__setattr__(self, param_name.upper(), param_value)
                    if ctx.get_parameter_source(param_name) == ParameterSource.DEFAULT:
                        params_assigned_from_default.add(param_name)

            if ctx.parent:
                assign_params(ctx.parent, params_assigned_from_default)

        params_assigned_from_default = set()  # type: Set[str]
        assign_params(ctx, params_assigned_from_default)

        # Get the invoked command flags
        invoked_subcommand_name = (
            ctx.invoked_subcommand if hasattr(ctx, "invoked_subcommand") else None
        )
        if invoked_subcommand_name is not None:
            invoked_subcommand = getattr(import_module("dbt.cli.main"), invoked_subcommand_name)
            invoked_subcommand.allow_extra_args = True
            invoked_subcommand.ignore_unknown_options = True
            invoked_subcommand_ctx = invoked_subcommand.make_context(None, sys.argv)
            assign_params(invoked_subcommand_ctx, params_assigned_from_default)

        if not user_config:
            profiles_dir = getattr(self, "PROFILES_DIR", None)
            user_config = read_user_config(profiles_dir) if profiles_dir else None

        # Overwrite default assignments with user config if available
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

        # Hard coded flags
        object.__setattr__(self, "WHICH", invoked_subcommand_name or ctx.info_name)
        object.__setattr__(self, "MP_CONTEXT", get_context("spawn"))

        # Apply the lead/follow relationship between some parameters
        self._override_if_set("USE_COLORS", "USE_COLORS_FILE", params_assigned_from_default)
        self._override_if_set("LOG_LEVEL", "LOG_LEVEL_FILE", params_assigned_from_default)
        self._override_if_set("LOG_FORMAT", "LOG_FORMAT_FILE", params_assigned_from_default)

        # Default LOG_PATH from PROJECT_DIR, if available.
        if getattr(self, "LOG_PATH", None) is None:
            project_dir = getattr(self, "PROJECT_DIR", default_project_dir())
            version_check = getattr(self, "VERSION_CHECK", True)
            object.__setattr__(self, "LOG_PATH", default_log_path(project_dir, version_check))

        # Support console DO NOT TRACK initiative
        if os.getenv("DO_NOT_TRACK", "").lower() in ("1", "t", "true", "y", "yes"):
            object.__setattr__(self, "SEND_ANONYMOUS_USAGE_STATS", False)

        # Check mutual exclusivity once all flags are set
        self._assert_mutually_exclusive(
            params_assigned_from_default, ["WARN_ERROR", "WARN_ERROR_OPTIONS"]
        )

        # Support lower cased access for legacy code
        params = set(
            x for x in dir(self) if not callable(getattr(self, x)) and not x.startswith("__")
        )
        for param in params:
            object.__setattr__(self, param.lower(), getattr(self, param))

    # If the value of the lead parameter was set explicitly, apply the value to follow,
    # unless follow was also set explicitly.
    def _override_if_set(self, lead: str, follow: str, defaulted: Set[str]) -> None:
        if lead.lower() not in defaulted and follow.lower() in defaulted:
            object.__setattr__(self, follow.upper(), getattr(self, lead.upper(), None))

    def __str__(self) -> str:
        return str(pf(self.__dict__))

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
                raise BadOptionUsage(
                    flag.lower(), f"{flag.lower()}: not allowed with argument {set_flag.lower()}"
                )
            elif flag_set_by_user:
                set_flag = flag
