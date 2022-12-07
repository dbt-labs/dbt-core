# TODO  Move this to /core/dbt/flags.py when we're ready to break things
import os
import sys
from dataclasses import dataclass
from importlib import import_module
from multiprocessing import get_context
from pprint import pformat as pf
from typing import Set

from click import Context, get_current_context
from click.core import ParameterSource

from dbt.config.profile import read_user_config
from dbt.contracts.project import UserConfig

if os.name != "nt":
    # https://bugs.python.org/issue41567
    import multiprocessing.popen_spawn_posix  # type: ignore  # noqa: F401


@dataclass(frozen=True)
class Flags:
    def __init__(self, ctx: Context = None, user_config: UserConfig = None) -> None:

        if ctx is None:
            ctx = get_current_context()

        def assign_params(ctx, params_assigned_from_default):
            """Recursively adds all click params to flag object"""
            for param_name, param_value in ctx.params.items():
                # N.B. You have to use the base MRO method (object.__setattr__) to set attributes
                # when using frozen dataclasses.
                # https://docs.python.org/3/library/dataclasses.html#frozen-instances
                if hasattr(self, param_name):
                    raise Exception(f"Duplicate flag names found in click command: {param_name}")
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
            for param_assigned_from_default in params_assigned_from_default:
                user_config_param_value = getattr(user_config, param_assigned_from_default, None)
                if user_config_param_value is not None:
                    object.__setattr__(
                        self, param_assigned_from_default.upper(), user_config_param_value
                    )

        # Hard coded flags
        object.__setattr__(self, "WHICH", invoked_subcommand_name or ctx.info_name)
        object.__setattr__(self, "MP_CONTEXT", get_context("spawn"))

        # Support console DO NOT TRACK initiave
        object.__setattr__(
            self,
            "ANONYMOUS_USAGE_STATS",
            False
            if os.getenv("DO_NOT_TRACK", "").lower() in ("1", "t", "true", "y", "yes")
            else True,
        )

    def __str__(self) -> str:
        return str(pf(self.__dict__))
