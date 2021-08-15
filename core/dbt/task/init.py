import copy
import os
import shutil
from typing import Tuple

import oyaml as yaml
import click
from jinja2 import Template

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
Your new dbt project "{project_name}" was created!

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

    def create_profiles_dir(self, profiles_dir: str) -> bool:
        """Create the user's profiles directory if it doesn't already exist."""
        if not os.path.exists(profiles_dir):
            msg = "Creating dbt configuration folder at {}"
            logger.info(msg.format(profiles_dir))
            dbt.clients.system.make_directory(profiles_dir)
            return True
        return False

    def create_profile_from_sample(self, adapter: str):
        """Create a profile entry using the adapter's sample_profiles.yml"""
        # Line below raises an exception if the specified adapter is not found
        load_plugin(adapter)
        adapter_path = get_include_paths(adapter)[0]
        sample_profiles_path = adapter_path / "sample_profiles.yml"

        if not sample_profiles_path.exists():
            logger.debug(f"No sample profile found for {adapter}.")
        else:
            with open(sample_profiles_path, "r") as f:
                # Ignore the name given in the sample_profiles.yml
                profile = list(yaml.load(f).values())[0]
                profiles_filepath, profile_name = self.write_profile(profile)
                logger.info(
                    f"Profile {profile_name} written to {profiles_filepath} "
                    "using sample configuration. Once updated "
                    "you'll be able to start developing with dbt."
                )

    def get_addendum(self, project_name: str, profiles_path: str) -> str:
        open_cmd = dbt.clients.system.open_dir_cmd()

        return ON_COMPLETE_MESSAGE.format(
            open_cmd=open_cmd,
            project_name=project_name,
            profiles_path=profiles_path,
            docs_url=DOCS_URL,
            slack_url=SLACK_URL
        )

    def generate_target_from_input(
        self,
        target_options: dict,
        target: dict = {}
    ) -> dict:
        """Generate a target configuration from target_options and user input.
        """
        target_options_local = copy.deepcopy(target_options)
        for key, value in target_options_local.items():
            if key.startswith("_choose"):
                choice_type = key[8:].replace("_", " ")
                option_list = list(value.keys())
                options_msg = "\n".join([
                    f"[{n+1}] {v}" for n, v in enumerate(option_list)
                ])
                click.echo(options_msg)
                numeric_choice = click.prompt(
                    f"Desired {choice_type} option (enter a number)", type=click.INT
                )
                choice = option_list[numeric_choice - 1]
                # Complete the chosen option's values in a recursive call
                target = self.generate_target_from_input(
                    target_options_local[key][choice], target
                )
            else:
                if key.startswith("_fixed"):
                    # _fixed prefixed keys are not presented to the user
                    target[key[7:]] = value
                elif isinstance(value, str) and (value[0] + value[-1] == "[]"):
                    # A square bracketed value is used as a hint
                    hide_input = key == "password"
                    target[key] = click.prompt(
                        f"{key} ({value[1:-1]})", hide_input=hide_input
                    )
                elif isinstance(value, list):
                    # A list can be used to provide both a hint and a default
                    target[key] = click.prompt(
                        f"{key} ({value[0]})", default=value[1]
                    )
                else:
                    # All other values are used as defaults
                    target[key] = click.prompt(
                        key, default=target_options_local[key]
                    )
        return target

    def get_profile_name_from_current_project(self) -> str:
        """Reads dbt_project.yml in the current directory to retrieve the
        profile name.
        """
        with open("dbt_project.yml") as f:
            dbt_project = yaml.load(f)
        return dbt_project["profile"]

    def write_profile(
        self, profile: dict, profile_name: str = None
    ) -> Tuple[str, str]:
        """Given a profile, write it to the current project's profiles.yml.
        This will overwrite any profile with a matching name."""
        profiles_file = os.path.join(dbt.config.PROFILES_DIR, "profiles.yml")
        profile_name = (
            profile_name or self.get_profile_name_from_current_project()
        )
        if os.path.exists(profiles_file):
            with open(profiles_file, "r+") as f:
                profiles = yaml.load(f) or {}
                profiles[profile_name] = profile
                f.seek(0)
                yaml.dump(profiles, f)
                f.truncate()
        else:
            profiles = {profile_name: profile}
            with open(profiles_file, "w") as f:
                yaml.dump(profiles, f)
        return profiles_file, profile_name

    def create_profile_from_target_options(self, target_options: dict):
        """Create and write a profile using the supplied target_options."""
        target = self.generate_target_from_input(target_options)
        profile = {
            "outputs": {
                "dev": target
            },
            "target": "dev"
        }
        profiles_filepath, profile_name = self.write_profile(profile)
        logger.info(
            f"Profile {profile_name} written to {profiles_filepath} using "
            "your supplied values."
        )

    def create_profile_from_scratch(self, adapter: str):
        """Create a profile without defaults using target_options.yml if available, or
        sample_profiles.yml as a fallback."""
        # Line below raises an exception if the specified adapter is not found
        load_plugin(adapter)
        adapter_path = get_include_paths(adapter)[0]
        target_options_path = adapter_path / "target_options.yml"

        if target_options_path.exists():
            with open(target_options_path) as f:
                target_options = yaml.load(f)
            self.create_profile_from_target_options(target_options)
        else:
            # For adapters without a target_options.yml defined, fallback on
            # sample_profiles.yml
            self.create_profile_from_sample(adapter)

    def check_if_can_write_profile(self, profile_name: str = None) -> bool:
        profiles_file = os.path.join(dbt.config.PROFILES_DIR, "profiles.yml")
        if not os.path.exists(profiles_file):
            return True
        profile_name = (
            profile_name or self.get_profile_name_from_current_project()
        )
        with open(profiles_file, "r") as f:
            profiles = yaml.load(f) or {}
        if profile_name in profiles.keys():
            response = click.confirm(
                f"The profile {profile_name} already exists in "
                f"{profiles_file}. Continue and overwrite it?"
            )
            return response
        else:
            return True

    def create_profile_using_profile_template(self):
        """Create a profile using profile_template.yml"""
        with open("profile_template.yml") as f:
            profile_template = yaml.load(f)
        profile_name = list(profile_template["profile"].keys())[0]
        self.check_if_can_write_profile(profile_name)
        render_vars = {}
        for template_variable in profile_template["vars"]:
            render_vars[template_variable] = click.prompt(template_variable)
        profile = profile_template["profile"][profile_name]
        profile_str = yaml.dump(profile)
        profile_str = Template(profile_str).render(vars=render_vars)
        profile = yaml.load(profile_str)
        profiles_filepath, _ = self.write_profile(profile, profile_name)
        logger.info(
            f"Profile {profile_name} written to {profiles_filepath} using "
            "profile_template.yml and your supplied values."
        )

    def ask_for_adapter_choice(self) -> str:
        """Ask the user which adapter (database) they'd like to use."""
        click.echo("Which database would you like to use?")
        available_adapters = list(_get_adapter_plugin_names())
        click.echo("\n".join([
            f"[{n+1}] {v}" for n, v in enumerate(available_adapters)
        ]))
        numeric_choice = click.prompt("Enter a number", type=click.INT)
        return available_adapters[numeric_choice - 1]

    def run(self):
        profiles_dir = dbt.config.PROFILES_DIR
        self.create_profiles_dir(profiles_dir)

        try:
            move_to_nearest_project_dir(self.args)
            in_project = True
        except dbt.exceptions.RuntimeException:
            in_project = False

        if in_project:
            logger.info("Setting up your profile.")
            if os.path.exists("profile_template.yml"):
                self.create_profile_using_profile_template()
            else:
                if not self.check_if_can_write_profile():
                    return
                adapter = self.ask_for_adapter_choice()
                self.create_profile_from_scratch(
                    adapter
                )
        else:
            project_dir = click.prompt("What is the desired project name?")
            if os.path.exists(project_dir):
                logger.info(
                    f"Existing project found at directory {project_dir}"
                )
                return

            self.copy_starter_repo(project_dir)
            os.chdir(project_dir)
            if not self.check_if_can_write_profile():
                return
            adapter = self.ask_for_adapter_choice()
            self.create_profile_from_scratch(
                adapter
            )
            logger.info(self.get_addendum(project_dir, profiles_dir))
