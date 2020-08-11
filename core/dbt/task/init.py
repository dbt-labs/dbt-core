import os
import shutil

import dbt.config
import dbt.clients.git
import dbt.clients.system
from dbt.adapters.factory import load_plugin, get_include_paths
from dbt.exceptions import RuntimeException

from dbt.logger import GLOBAL_LOGGER as logger

from dbt.task.base import BaseTask

STARTER_REPO = 'https://github.com/fishtown-analytics/dbt-starter-project.git'
STARTER_BRANCH = 'dbt-yml-config-version-2'
DOCS_URL = 'https://docs.getdbt.com/docs/configure-your-profile'

ON_COMPLETE_MESSAGE = """
Your new dbt project "{project_name}" was created! If this is your first time
using dbt, you'll need to set up your profiles.yml file (we've created a sample
file for you to connect to {sample_adapter}) -- this file will tell dbt how
to connect to your database. You can find this file by running:

  {open_cmd} {profiles_path}

For more information on how to configure the profiles.yml file,
please consult the dbt documentation here:

  {docs_url}

One more thing:

Need help? Don't hesitate to reach out to us via GitHub issues or on Slack --
There's a link to our Slack group in the GitHub Readme. Happy modeling!
"""


class InitTask(BaseTask):
    def clone_starter_repo(self, project_name):
        dbt.clients.git.clone(
            STARTER_REPO,
            cwd='.',
            dirname=project_name,
            remove_git_dir=True,
            branch=STARTER_BRANCH,
        )

    def create_profiles_dir(self, profiles_dir):
        if not os.path.exists(profiles_dir):
            dbt.clients.system.make_directory(profiles_dir)
            return True
        return False

    def create_profiles_file(self, profiles_file, sample_adapter):
        # Line below raises an exception if the specified adapter is not found
        load_plugin(sample_adapter)
        adapter_path = get_include_paths(sample_adapter)[0]
        sample_profiles_path = adapter_path / 'sample_profiles.yml'

        if not sample_profiles_path.exists():
            raise RuntimeException(f'No sample profile for {sample_adapter}')

        if not os.path.exists(profiles_file):
            shutil.copyfile(sample_profiles_path, profiles_file)
            return True

        return False

    def get_addendum(self, project_name, profiles_path, sample_adapter):
        open_cmd = dbt.clients.system.open_dir_cmd()

        return ON_COMPLETE_MESSAGE.format(
            open_cmd=open_cmd,
            project_name=project_name,
            sample_adapter=sample_adapter,
            profiles_path=profiles_path,
            docs_url=DOCS_URL
        )

    def run(self):
        project_dir = self.args.project_name
        sample_adapter = self.args.adapter

        profiles_dir = dbt.config.PROFILES_DIR
        profiles_file = os.path.join(profiles_dir, 'profiles.yml')

        msg = "Creating dbt configuration folder at {}"
        logger.info(msg.format(profiles_dir))

        msg = "With sample profiles.yml for {}"
        logger.info(msg.format(sample_adapter))

        self.create_profiles_dir(profiles_dir)
        self.create_profiles_file(profiles_file, sample_adapter)

        if os.path.exists(project_dir):
            raise RuntimeError("directory {} already exists!".format(
                project_dir
            ))

        self.clone_starter_repo(project_dir)

        addendum = self.get_addendum(project_dir, profiles_dir, sample_adapter)
        logger.info(addendum)
