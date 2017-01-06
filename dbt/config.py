import os.path
import yaml

import dbt.project as project


def read_config(profiles_dir):
    # TODO: validate profiles_dir
    path = os.path.join(profiles_dir, 'profiles.yml')

    if os.path.isfile(path):
        with open(path, 'r') as f:
            profile = yaml.safe_load(f)
            return profile.get('config', {})

    return {}


def send_anonymous_usage_stats(profiles_dir):
    config = read_config(profiles_dir)

    if config is not None \
       and not config.get("send_anonymous_usage_stats", True):
        return False

    return True
