import os.path

import dbt.exceptions
import dbt.clients.yaml_helper
import dbt.clients.system

from dbt.logger import GLOBAL_LOGGER as logger
from dbt.utils import DBTConfigKeys


INVALID_PROFILE_MESSAGE = """
dbt encountered an error while trying to read your profiles.yml file.

{error_string}
"""

UNUSED_RESOURCE_CONFIGURATION_PATH_MESSAGE = """\
WARNING: Configuration paths exist in your dbt_project.yml file which do not \
apply to any resources.
There are {} unused configuration paths:\
"""


def read_profile(profiles_dir):
    path = os.path.join(profiles_dir, 'profiles.yml')

    contents = None
    if os.path.isfile(path):
        try:
            contents = dbt.clients.system.load_file_contents(path, strip=False)
            return dbt.clients.yaml_helper.load_yaml_text(contents)
        except dbt.exceptions.ValidationException as e:
            msg = INVALID_PROFILE_MESSAGE.format(error_string=e)
            raise dbt.exceptions.ValidationException(msg)

    return {}


def read_config(profiles_dir):
    profile = read_profile(profiles_dir)
    if profile is None:
        return {}
    else:
        return profile.get('config', {})


def send_anonymous_usage_stats(config):
    return config.get('send_anonymous_usage_stats', True)


def colorize_output(config):
    return config.get('use_colors', True)


def get_config_paths(config, path=None, paths=None):
    if path is None:
        path = []

    if paths is None:
        paths = []

    for key, value in config.items():
        if isinstance(value, dict):
            if key in DBTConfigKeys:
                if path not in paths:
                    paths.append(path)
            else:
                get_config_paths(value, path + [key], paths)
        else:
            if path not in paths:
                paths.append(path)

    return paths


def get_project_resource_config_paths(project):
    resource_config_paths = {}
    for resource_type in ['models', 'seeds']:
        if resource_type in project:
            resource_config_paths[resource_type] = get_config_paths(
                project[resource_type])
    return resource_config_paths


def is_config_used(config_path, fqns):
    for fqn in fqns:
        if len(config_path) <= len(fqn) and fqn[:len(config_path)] == config_path:
            return True
    return False


def get_unused_resource_config_paths(resource_config_paths, resource_fqns):
    unused_resource_config_paths = []
    for resource_type, config_paths in resource_config_paths.items():
        for config_path in config_paths:
            if not is_config_used(config_path, resource_fqns[resource_type]):
                unused_resource_config_paths.append(
                    [resource_type] + config_path)
    return unused_resource_config_paths


def warn_for_unused_resource_config_paths(resource_config_paths, resource_fqns):
    unused_resource_config_paths = get_unused_resource_config_paths(
        resource_config_paths, resource_fqns)
    if len(unused_resource_config_paths) == 0:
        return
    logger.info(
        dbt.ui.printer.yellow(UNUSED_RESOURCE_CONFIGURATION_PATH_MESSAGE.format(
            len(unused_resource_config_paths))))
    for unused_resource_config_path in unused_resource_config_paths:
        logger.info(
            dbt.ui.printer.yellow(" - {}".format(
                ".".join(unused_resource_config_path))))
    logger.info("")
