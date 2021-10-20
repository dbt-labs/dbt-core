import copy
import os
from pathlib import Path
import re
import shutil
from typing import Optional

import yaml
import click
from jinja2 import Template

import dbt.config
import dbt.clients.system
from dbt import flags
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

# https://click.palletsprojects.com/en/8.0.x/api/?highlight=float#types
click_type_mapping = {
    "string": click.STRING,
    "int": click.INT,
    "float": click.FLOAT,
    "bool": click.BOOL,
    None: None
}


class InitTask(BaseTask):
    def copy_starter_repo(self, project_name):
        logger.debug("Starter project path: " + starter_project_directory)
        shutil.copytree(starter_project_directory, project_name,
                        ignore=shutil.ignore_patterns(*IGNORE_FILES))

    def create_profiles_dir(self, profiles_dir: str) -> bool:
        """Create the user's profiles directory if it doesn't already exist."""
        profiles_path = Path(profiles_dir)
        if profiles_path.exists():
            msg = "Creating dbt configuration folder at {}"
            logger.info(msg.format(profiles_dir))
            dbt.clients.system.make_directory(profiles_dir)
            return True
        return False

    def create_profile_from_sample(self, adapter: str, profile_name: str):
        """Create a profile entry using the adapter's sample_profiles.yml

        Renames the profile in sample_profiles.yml to match that of the project."""
        # Line below raises an exception if the specified adapter is not found
        load_plugin(adapter)
        adapter_path = get_include_paths(adapter)[0]
        sample_profiles_path = adapter_path / "sample_profiles.yml"

        if not sample_profiles_path.exists():
            logger.debug(f"No sample profile found for {adapter}.")
        else:
            with open(sample_profiles_path, "r") as f:
                sample_profile = f.read()
            sample_profile_name = list(yaml.safe_load(sample_profile).keys())[0]
            # Use a regex to replace the name of the sample_profile with
            # that of the project without losing any comments from the sample
            sample_profile = re.sub(
                f"^{sample_profile_name}:",
                f"{profile_name}:",
                sample_profile
            )
            profiles_filepath = Path(flags.PROFILES_DIR) / Path("profiles.yml")
            if profiles_filepath.exists():
                with open(profiles_filepath, "a") as f:
                    f.write("\n" + sample_profile)
            else:
                with open(profiles_filepath, "w") as f:
                    f.write(sample_profile)
                logger.info(
                    f"Profile {profile_name} written to {profiles_filepath} "
                    "using sample configuration. Once updated, you'll be able to "
                    "start developing with dbt."
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
                prompt_msg = "\n".join([
                    f"[{n+1}] {v}" for n, v in enumerate(option_list)
                ]) + f"\nDesired {choice_type} option (enter a number)"
                numeric_choice = click.prompt(prompt_msg, type=click.INT)
                choice = option_list[numeric_choice - 1]
                # Complete the chosen option's values in a recursive call
                target = self.generate_target_from_input(
                    target_options_local[key][choice], target
                )
            else:
                if key.startswith("_fixed"):
                    # _fixed prefixed keys are not presented to the user
                    target[key[7:]] = value
                else:
                    hide_input = value.get("hide_input", False)
                    default = value.get("default", None)
                    hint = value.get("hint", None)
                    type = click_type_mapping[value.get("type", None)]
                    text = key + (f" ({hint})" if hint else "")
                    target[key] = click.prompt(
                        text,
                        default=default,
                        hide_input=hide_input,
                        type=type
                    )
        return target

    def get_profile_name_from_current_project(self) -> str:
        """Reads dbt_project.yml in the current directory to retrieve the
        profile name.
        """
        with open("dbt_project.yml") as f:
            dbt_project = yaml.safe_load(f)
        return dbt_project["profile"]

    def write_profile(
        self, profile: dict, profile_name: str
    ) -> Path:
        """Given a profile, write it to the current project's profiles.yml.
        This will overwrite any profile with a matching name."""
        # Create the profile directory if it doesn't exist
        os.makedirs(flags.PROFILES_DIR, exist_ok=True)
        profiles_filepath = Path(flags.PROFILES_DIR) / Path("profiles.yml")
        if profiles_filepath.exists():
            with open(profiles_filepath, "r+") as f:
                profiles = yaml.safe_load(f) or {}
                profiles[profile_name] = profile
                f.seek(0)
                yaml.dump(profiles, f)
                f.truncate()
        else:
            profiles = {profile_name: profile}
            with open(profiles_filepath, "w") as f:
                yaml.dump(profiles, f)
        return profiles_filepath

    def create_profile_from_target_options(self, target_options: dict, profile_name: str):
        """Create and write a profile using the supplied target_options."""
        target = self.generate_target_from_input(target_options)
        profile = {
            "outputs": {
                "dev": target
            },
            "target": "dev"
        }
        profiles_filepath = self.write_profile(profile, profile_name)
        logger.info(
            f"Profile {profile_name} written to {profiles_filepath} using "
            "your supplied values. Run 'dbt debug' to validate the connection."
        )

    def create_profile_from_scratch(self, adapter: str, profile_name: str):
        """Create a profile without defaults using target_options.yml if available, or
        sample_profiles.yml as a fallback."""
        # Line below raises an exception if the specified adapter is not found
        load_plugin(adapter)
        adapter_path = get_include_paths(adapter)[0]
        target_options_path = adapter_path / "target_options.yml"

        if target_options_path.exists():
            with open(target_options_path) as f:
                target_options = yaml.safe_load(f)
            self.create_profile_from_target_options(target_options, profile_name)
        else:
            # For adapters without a target_options.yml defined, fallback on
            # sample_profiles.yml
            self.create_profile_from_sample(adapter, profile_name)

    def check_if_can_write_profile(self, profile_name: Optional[str] = None) -> bool:
        """Using either a provided profile name or that specified in dbt_project.yml,
        check if the profile already exists in profiles.yml, and if so ask the
        user whether to proceed and overwrite it."""
        profiles_file = Path(flags.PROFILES_DIR) / Path("profiles.yml")
        if not profiles_file.exists():
            return True
        profile_name = (
            profile_name or self.get_profile_name_from_current_project()
        )
        with open(profiles_file, "r") as f:
            profiles = yaml.safe_load(f) or {}
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
            profile_template = yaml.safe_load(f)
        profile_name = list(profile_template["profile"].keys())[0]
        self.check_if_can_write_profile(profile_name)
        render_vars = {}
        for template_variable in profile_template["prompts"]:
            render_vars[template_variable] = click.prompt(template_variable)
        profile = profile_template["profile"][profile_name]
        profile_str = yaml.dump(profile)
        profile_str = Template(profile_str).render(render_vars)
        profile = yaml.safe_load(profile_str)
        profiles_filepath = self.write_profile(profile, profile_name)
        logger.info(
            f"Profile {profile_name} written to {profiles_filepath} using "
            "profile_template.yml and your supplied values. Run 'dbt debug' "
            "to validate the connection."
        )

    def ask_for_adapter_choice(self) -> str:
        """Ask the user which adapter (database) they'd like to use."""
        available_adapters = list(_get_adapter_plugin_names())
        prompt_msg = (
            "Which database would you like to use?\n" +
            "\n".join([f"[{n+1}] {v}" for n, v in enumerate(available_adapters)]) +
            "\n\n(Don't see the one you want? https://docs.getdbt.com/docs/available-adapters)" +
            "\n\nEnter a number"
        )
        numeric_choice = click.prompt(prompt_msg, type=click.INT)
        return available_adapters[numeric_choice - 1]

    def run(self):
        """Entry point for the init task."""
        profiles_dir = flags.PROFILES_DIR
        self.create_profiles_dir(profiles_dir)

        try:
            move_to_nearest_project_dir(self.args)
            in_project = True
        except dbt.exceptions.RuntimeException:
            in_project = False

        if in_project:
            # When dbt init is run inside an existing project,
            # just setup the user's profile.
            logger.info("Setting up your profile.")
            profile_name = self.get_profile_name_from_current_project()
            profile_template_path = Path("profile_template.yml")
            if profile_template_path.exists():
                try:
                    # This relies on a valid profile_template.yml from the user,
                    # so use a try: except to fall back to the default on failure
                    self.create_profile_using_profile_template()
                    return
                except Exception:
                    logger.info("Invalid profile_template.yml in project.")
            if not self.check_if_can_write_profile(profile_name=profile_name):
                return
            adapter = self.ask_for_adapter_choice()
            self.create_profile_from_scratch(
                adapter, profile_name=profile_name
            )
        else:
            # When dbt init is run outside of an existing project,
            # create a new project and set up the user's profile.
            project_name = click.prompt("What is the desired project name?")
            project_path = Path(project_name)
            if project_path.exists():
                logger.info(
                    f"A project called {project_name} already exists here."
                )
                return

            self.copy_starter_repo(project_name)
            os.chdir(project_name)
            with open("dbt_project.yml", "r+") as f:
                content = f"{f.read()}".format(
                    project_name=project_name,
                    profile_name=project_name
                )
                f.seek(0)
                f.write(content)
                f.truncate()

            if not self.check_if_can_write_profile(profile_name=project_name):
                return
            adapter = self.ask_for_adapter_choice()
            self.create_profile_from_scratch(
                adapter, profile_name=project_name
            )
            logger.info(self.get_addendum(project_name, profiles_dir))
