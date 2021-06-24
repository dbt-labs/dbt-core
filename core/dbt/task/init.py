import os
import shutil

import dbt.config
import dbt.clients.system
from dbt.version import _get_adapter_plugin_names
from dbt.adapters.factory import load_plugin, get_include_paths

from dbt.logger import GLOBAL_LOGGER as logger

from dbt.include.starter_project import PACKAGE_PATH as starter_project_directory

from dbt.task.base import BaseTask

DOCS_URL = 'https://docs.getdbt.com/docs/configure-your-profile'
SLACK_URL = 'https://community.getdbt.com/'

# This file is not needed for the starter project but exists for finding the resource path
IGNORE_FILE = "__init__.py"

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
                        ignore=shutil.ignore_patterns(IGNORE_FILE))

    def create_profiles_dir(self, profiles_dir):
        if not os.path.exists(profiles_dir):
            msg = "Creating dbt configuration folder at {}"
            logger.info(msg.format(profiles_dir))
            dbt.clients.system.make_directory(profiles_dir)
            return True
        return False

    def create_profiles_file(self, profiles_file, sample_adapter):
        # Line below raises an exception if the specified adapter is not found
        load_plugin(sample_adapter)
        adapter_path = get_include_paths(sample_adapter)[0]
        sample_profiles_path = adapter_path / 'sample_profiles.yml'

        if not sample_profiles_path.exists():
            logger.debug(f"No sample profile found for {sample_adapter}, skipping")
            return False

        if not os.path.exists(profiles_file):
            msg = "With sample profiles.yml for {}"
            logger.info(msg.format(sample_adapter))
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

    def run(self):
        project_dir = self.args.project_name
        sample_adapter = self.args.adapter
        if not sample_adapter:
            try:
                # pick first one available, often postgres
                sample_adapter = next(_get_adapter_plugin_names())
            except StopIteration:
                logger.debug("No adapters installed, skipping")

        profiles_dir = dbt.config.PROFILES_DIR
        profiles_file = os.path.join(profiles_dir, 'profiles.yml')

        self.create_profiles_dir(profiles_dir)
        if sample_adapter:
            self.create_profiles_file(profiles_file, sample_adapter)

        if os.path.exists(project_dir):
            raise RuntimeError("directory {} already exists!".format(
                project_dir
            ))

        self.copy_starter_repo(project_dir)

        addendum = self.get_addendum(project_dir, profiles_dir)
        logger.info(addendum)
