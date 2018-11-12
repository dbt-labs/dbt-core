import os

import dbt.config
import dbt.clients.git
import dbt.clients.system

from dbt.logger import GLOBAL_LOGGER as logger

from dbt.task.base_task import BaseTask

STARTER_REPO = 'https://github.com/fishtown-analytics/dbt-starter-project.git'
DOCS_URL = 'https://docs.getdbt.com/docs/configure-your-profile'
SAMPLE_PROFILES_YML_FILE = 'https://github.com/fishtown-analytics/dbt/blob/master/sample.profiles.yml'  # noqa

ON_COMPLETE_MESSAGE = """
Your new dbt project "{project_name}" was created! If this is your first time
using dbt, you'll need to set up your profiles.yml file -- this file will
tell dbt how to connect to your database. You can find this file by running:

  {open_cmd} {profiles_path}

For more information on how to configure the profiles.yml file,
please consult the dbt documentation here:

  {docs_url}

One more thing:

Need help? Don't hesitate to reach out to us via GitHub issues or on Slack --
There's a link to our Slack group in the GitHub Readme. Happy modeling!
"""


STARTER_PROFILE = """
# For more information on how to configure this file, please see:
# {profiles_sample}

default:
  outputs:
    dev:
      type: redshift
      threads: 1
      host: 127.0.0.1
      port: 5439
      user: alice
      pass: pa55word
      dbname: warehouse
      schema: dbt_alice
    prod:
      type: redshift
      threads: 1
      host: 127.0.0.1
      port: 5439
      user: alice
      pass: pa55word
      dbname: warehouse
      schema: analytics
  target: dev
""".format(profiles_sample=SAMPLE_PROFILES_YML_FILE)


class InitTask(BaseTask):
    def clone_starter_repo(self, project_name):
        dbt.clients.git.clone(
            STARTER_REPO, '.', project_name,
            remove_git_dir=True)
        dbt.clients.git.remove_remote(project_name)

    def create_profiles_dir(self, profiles_dir):
        if not os.path.exists(profiles_dir):
            dbt.clients.system.make_directory(profiles_dir)
            return True
        return False

    def create_profiles_file(self, profiles_file):
        if not os.path.exists(profiles_file):
            dbt.clients.system.make_file(profiles_file, STARTER_PROFILE)
            return True
        return False

    def get_addendum(self, project_name, profiles_path):
        open_cmd = dbt.clients.system.open_dir_cmd()

        return ON_COMPLETE_MESSAGE.format(
            open_cmd=open_cmd,
            project_name=project_name,
            profiles_path=profiles_path,
            docs_url=DOCS_URL
        )

    def run(self):
        project_dir = self.args.project_name

        profiles_dir = dbt.config.PROFILES_DIR
        profiles_file = os.path.join(profiles_dir, 'profiles.yml')

        self.create_profiles_dir(profiles_dir)
        self.create_profiles_file(profiles_file)

        msg = "Creating dbt configuration folder at {}"
        logger.info(msg.format(profiles_dir))

        if os.path.exists(project_dir):
            raise RuntimeError("directory {} already exists!".format(
                project_dir
            ))

        self.clone_starter_repo(project_dir)

        addendum = self.get_addendum(project_dir, profiles_dir)
        logger.info(addendum)
