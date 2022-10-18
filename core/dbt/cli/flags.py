# TODO  Move this to /core/dbt/flags.py when we're ready to break things
import os
import sys
from dataclasses import dataclass
from importlib import import_module
from multiprocessing import get_context
from pprint import pformat as pf

from click import Context, get_current_context

if os.name != "nt":
    # https://bugs.python.org/issue41567
    import multiprocessing.popen_spawn_posix  # type: ignore  # noqa: F401


@dataclass(frozen=True)
class Flags:
    def __init__(self, ctx: Context = None) -> None:

        if ctx is None:
            ctx = get_current_context()

        def assign_params(ctx):
            """Recursively adds all click params to flag object"""
            for param_name, param_value in ctx.params.items():
                # N.B. You have to use the base MRO method (object.__setattr__) to set attributes
                # when using frozen dataclasses.
                # https://docs.python.org/3/library/dataclasses.html#frozen-instances
                if hasattr(self, param_name):
                    raise Exception(f"Duplicate flag names found in click command: {param_name}")
                object.__setattr__(self, param_name.upper(), param_value)
            if ctx.parent:
                assign_params(ctx.parent)

        assign_params(ctx)

        # Get the invoked command flags
        if hasattr(ctx, "invoked_subcommand") and ctx.invoked_subcommand is not None:
            invoked_subcommand = getattr(import_module("dbt.cli.main"), ctx.invoked_subcommand)
            invoked_subcommand.allow_extra_args = True
            invoked_subcommand.ignore_unknown_options = True
            invoked_subcommand_ctx = invoked_subcommand.make_context(None, sys.argv)
            assign_params(invoked_subcommand_ctx)

        # Hard coded flags
        object.__setattr__(self, "WHICH", ctx.info_name)
        object.__setattr__(self, "MP_CONTEXT", get_context("spawn"))

        # Support console DO NOT TRACK initiave
        if os.getenv("DO_NOT_TRACK", "").lower() in (1, "t", "true", "y", "yes"):
            object.__setattr__(self, "ANONYMOUS_USAGE_STATS", False)

    def __str__(self) -> str:
        return str(pf(self.__dict__))
