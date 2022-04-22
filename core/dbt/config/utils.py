from argparse import Namespace
from typing import Any, Dict, Optional, Union
from xmlrpc.client import Boolean
from dbt.contracts.project import UserConfig

import dbt.flags as flags
from dbt.clients import yaml_helper
from dbt.config import Profile, Project, read_user_config
from dbt.config.renderer import DbtProjectYamlRenderer, ProfileRenderer
from dbt.events.functions import fire_event
from dbt.events.types import InvalidVarsYAML
from dbt.exceptions import ValidationException, raise_compiler_error


def parse_cli_vars(var_string: str) -> Dict[str, Any]:
    try:
        cli_vars = yaml_helper.load_yaml_text(var_string)
        var_type = type(cli_vars)
        if var_type is dict:
            return cli_vars
        else:
            type_name = var_type.__name__
            raise_compiler_error(
                "The --vars argument must be a YAML dictionary, but was "
                "of type '{}'".format(type_name)
            )
    except ValidationException:
        fire_event(InvalidVarsYAML())
        raise


def get_project_config(
    project_path: str,
    profile_name: str,
    args: Namespace = Namespace(),
    cli_vars: Optional[Dict[str, Any]] = None,
    profile: Optional[Profile] = None,
    user_config: Optional[UserConfig] = None,
    return_dict: Boolean = True,
) -> Union[Project, Dict]:
    """Returns a project config (dict or object) from a given project path and profile name.

    Args:
        project_path: Path to project
        profile_name: Name of profile
        args: An argparse.Namespace that represents what would have been passed in on the
            command line (optional)
        cli_vars: A dict of any vars that would have been passed in on the command line (optional)
            (see parse_cli_vars above for formatting details)
        profile: A dbt.config.profile.Profile object (optional)
        user_config: A dbt.contracts.project.UserConfig object (optional)
        return_dict: Return a dict if true, return the full dbt.config.project.Project object if false

    Returns:
        A full project config

    """
    # Generate a profile if not provided
    if profile is None:
        # Generate user_config if not provided
        if user_config is None:
            user_config = read_user_config(flags.PROFILES_DIR)
        # Update flags
        flags.set_from_args(args, user_config)
        if cli_vars is None:
            cli_vars = {}
        profile = Profile.render_from_args(args, ProfileRenderer(cli_vars), profile_name)
    # Generate a project
    project = Project.from_project_root(
        project_path,
        DbtProjectYamlRenderer(profile),
        verify_version=bool(flags.VERSION_CHECK),
    )
    # Return
    return project.to_project_config() if return_dict else project
