import os.path
import yaml
import yaml.scanner

import dbt.exceptions

from dbt.logger import GLOBAL_LOGGER as logger


def read_profile(profiles_dir):
    # TODO: validate profiles_dir
    path = os.path.join(profiles_dir, 'profiles.yml')

    if os.path.isfile(path):
        try:
            with open(path, 'r') as f:
                return yaml.safe_load(f)
        except (yaml.scanner.ScannerError,
                yaml.YAMLError) as e:
            raise dbt.exceptions.ValidationException(
                '  Could not read {}\n\n{}'.format(path, str(e)))

    return {}


def read_config(profiles_dir):
    profile = read_profile(profiles_dir)
    return profile.get('config')


def send_anonymous_usage_stats(profiles_dir):
    config = read_config(profiles_dir)

    if config is not None \
       and not config.get("send_anonymous_usage_stats", True):
        return False

    return True
