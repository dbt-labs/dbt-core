import copy
import os
import shutil
import yaml

import click

import dbt.config
import dbt.clients.system
from dbt.version import _get_adapter_plugin_names
from dbt.adapters.factory import load_plugin, get_include_paths

from dbt.logger import GLOBAL_LOGGER as logger

from dbt.include.starter_project import PACKAGE_PATH as starter_project_directory

from dbt.task.base import BaseTask, move_to_nearest_project_dir

DOCS_URL = 'https://docs.getdbt.com/docs/configure-your-profile'
SLACK_URL = 'https://community.getdbt.com/'

# This file is not needed for the starter project but exists for finding the resource path
IGNORE_FILES = ["__init__.py", "__pycache__"]

ON_COMPLETE_MESSAGE = """
Your new dbt project "{project_name}" was created! If this is your first time
using dbt, you'll need to set up your profiles.yml file -- this file will tell dbt how
to connect to your database. You can find this file by running:

  {open_cmd} {profiles_path}

For more information on how to configure the profiles.yml file,
please consult the dbt documentation here:

  {docs_url}

One more thing:

Need help? Don't hesitate to reach out to us via GitHub issues or on Slack:

  {slack_url}

Happy modeling!
"""


class InitTask(BaseTask):
    def copy_starter_repo(self, project_name):
        logger.debug("Starter project path: " + starter_project_directory)
        shutil.copytree(starter_project_directory, project_name,
                        ignore=shutil.ignore_patterns(*IGNORE_FILES))

    def create_profiles_dir(self, profiles_dir):
        if not os.path.exists(profiles_dir):
            msg = "Creating dbt configuration folder at {}"
            logger.info(msg.format(profiles_dir))
            dbt.clients.system.make_directory(profiles_dir)
            return True
        return False

    def create_sample_profiles_file(self, profiles_file, adapter):
        # Line below raises an exception if the specified adapter is not found
        load_plugin(adapter)
        adapter_path = get_include_paths(adapter)[0]
        sample_profiles_path = adapter_path / 'sample_profiles.yml'

        if not sample_profiles_path.exists():
            logger.debug(f"No sample profile found for {adapter}, skipping")
            return False

        if not os.path.exists(profiles_file):
            logger.info(f"With sample profiles.yml for {adapter}")
            shutil.copyfile(sample_profiles_path, profiles_file)
            return True

        return False

    def get_addendum(self, project_name, profiles_path):
        open_cmd = dbt.clients.system.open_dir_cmd()

        return ON_COMPLETE_MESSAGE.format(
            open_cmd=open_cmd,
            project_name=project_name,
            profiles_path=profiles_path,
            docs_url=DOCS_URL,
            slack_url=SLACK_URL
        )

    def generate_target_from_input(self, target_options, target={}):
        target_options_local = copy.deepcopy(target_options)
        # value = click.prompt('Please enter a valid integer', type=int)
        for key, value in target_options_local.items():
            if not key.startswith("_"):
                if isinstance(value, str) and (value[0] + value[-1] == "[]"):
                    hide_input = key == "password"
                    target[key] = click.prompt(
                        f"{key} ({value[1:-1]})", hide_input=hide_input
                    )
                else:
                    target[key] = target_options_local[key]
            if key.startswith("_choose"):
                choice_type = key[8:]
                option_list = list(value.keys())
                options_msg = "\n".join([
                    f"[{n+1}] {v}" for n, v in enumerate(option_list)
                ])
                click.echo(options_msg)
                numeric_choice = click.prompt(
                    f"desired {choice_type} option (enter a number)", type=int
                )
                choice = option_list[numeric_choice - 1]
                target = self.generate_target_from_input(
                    target_options_local[key][choice], target
                )
        return target

    def get_profile_name_from_current_project(self):
        with open("dbt_project.yml") as f:
            dbt_project = yaml.load(f)
        return dbt_project["profile"]

    def write_profile(self, profiles_file, profile, profile_name=None):
        if not profile_name:
            profile_name = self.get_profile_name_from_current_project()
        if os.path.exists(profiles_file):
            with open(profiles_file, "r+") as f:
                profiles = yaml.load(f) or {}
                profiles[profile_name] = profile
                f.seek(0)
                yaml.dump(profiles, f)
        else:
            profiles = {profile_name: profile}
            with open(profiles_file, "w") as f:
                yaml.dump(profiles, f)

    def configure_profile_from_scratch(self, selected_adapter):
        # Line below raises an exception if the specified adapter is not found
        load_plugin(selected_adapter)
        adapter_path = get_include_paths(selected_adapter)[0]
        target_options_path = adapter_path / 'target_options.yml'
        profiles_file = os.path.join(dbt.config.PROFILES_DIR, 'profiles.yml')

        if not target_options_path.exists():
            logger.info(f"No options found for {selected_adapter}, using " +
                        "sample profiles instead. Make sure to update it at" +
                        "{profiles_file}.")
            self.create_sample_profiles_file(profiles_file, selected_adapter)
        else:
            logger.info(f"Using {selected_adapter} profile options.")
            with open(target_options_path) as f:
                target_options = yaml.load(f)
            target = self.generate_target_from_input(target_options)
            profile = {
                "outputs": {
                    "dev": target
                },
                "target": "dev"
            }
            self.write_profile(profiles_file, profile)

    def configure_profile_using_defaults(self, selected_adapter):
        raise(NotImplementedError())

    def run(self):
        selected_adapter = self.args.adapter
        profiles_dir = dbt.config.PROFILES_DIR
        self.create_profiles_dir(profiles_dir)

        # Determine whether we're initializing a new project or configuring a
        # profile for an existing one
        if self.args.project_name:
            project_dir = self.args.project_name
            if os.path.exists(project_dir):
                raise RuntimeError("directory {} already exists!".format(
                    project_dir
                ))

            self.copy_starter_repo(project_dir)

            addendum = self.get_addendum(project_dir, profiles_dir)
            logger.info(addendum)
            if not selected_adapter:
                try:
                    # pick first one available, often postgres
                    selected_adapter = next(_get_adapter_plugin_names())
                except StopIteration:
                    logger.debug("No adapters installed, skipping")
            self.configure_profile_from_scratch(
                selected_adapter
            )
        else:
            logger.info("Setting up your profile.")
            move_to_nearest_project_dir(self.args)
            if os.path.exists("target_defaults.yml"):
                self.configure_profile_using_defaults()
            else:
                if not selected_adapter:
                    raise RuntimeError("No adapter specified.")
                logger.info("Configuring from scratch.")
                self.configure_profile_from_scratch(
                    selected_adapter
                )
