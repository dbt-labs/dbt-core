# Do not import the os package because we expose this package in jinja
from argparse import Namespace
from pathlib import Path

# this roughly follows the patten of EVENT_MANAGER in dbt/common/events/functions.py
# During de-globlization, we'll need to handle both similarly
# Match USE_COLORS default with default in dbt.cli.params.use_colors for use in --version
GLOBAL_FLAGS = Namespace(USE_COLORS=True)  # type: ignore


def set_flags(flags):
    global GLOBAL_FLAGS
    GLOBAL_FLAGS = flags


def get_flags():
    return GLOBAL_FLAGS


def set_from_args(args: Namespace, project_flags):
    global GLOBAL_FLAGS
    from dbt.cli.flags import Flags, convert_config
    from dbt.cli.main import cli

    # we set attributes of args after initialize the flags, but project_flags
    # is being read in the Flags constructor, so we need to read it here and pass in
    # to make sure we use the correct project_flags
    profiles_dir = getattr(args, "PROFILES_DIR", None) or getattr(args, "profiles_dir", None)
    project_dir = getattr(args, "PROJECT_DIR", None) or getattr(args, "project_dir", None)
    if profiles_dir and project_dir:
        from dbt.config.project import read_project_flags

        project_flags = read_project_flags(project_dir, profiles_dir)

    # make a dummy context to get the flags, totally arbitrary
    ctx = cli.make_context("run", ["run"])
    flags = Flags(ctx, project_flags)
    for arg_name, args_param_value in vars(args).items():
        args_param_value = convert_config(arg_name, args_param_value)
        object.__setattr__(flags, arg_name.upper(), args_param_value)
        object.__setattr__(flags, arg_name.lower(), args_param_value)
    flags.set_common_global_flags()
    GLOBAL_FLAGS = flags  # type: ignore


def get_flag_dict():
    flag_attr = {
        "use_experimental_parser",
        "static_parser",
        "warn_error",
        "warn_error_options",
        "write_json",
        "partial_parse",
        "use_colors",
        "profiles_dir",
        "debug",
        "log_format",
        "version_check",
        "fail_fast",
        "send_anonymous_usage_stats",
        "printer_width",
        "indirect_selection",
        "log_cache_events",
        "quiet",
        "no_print",
        "cache_selected_only",
        "introspect",
        "target_path",
        "log_path",
        "invocation_command",
        "empty",
    }
    return {key: getattr(GLOBAL_FLAGS, key.upper(), None) for key in flag_attr}


# This is used by core/dbt/context/base.py to return a flag object
# in Jinja.
def get_flag_obj():
    new_flags = Namespace()
    for key, val in get_flag_dict().items():
        if isinstance(val, Path):
            val = str(val)
        setattr(new_flags, key.upper(), val)
    # The following 3 are CLI arguments only so they're not full-fledged flags,
    # but we put in flags for users.
    setattr(new_flags, "FULL_REFRESH", getattr(GLOBAL_FLAGS, "FULL_REFRESH", None))
    setattr(new_flags, "STORE_FAILURES", getattr(GLOBAL_FLAGS, "STORE_FAILURES", None))
    setattr(new_flags, "WHICH", getattr(GLOBAL_FLAGS, "WHICH", None))
    return new_flags
