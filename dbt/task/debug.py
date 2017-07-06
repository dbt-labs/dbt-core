import pprint

from dbt.logger import GLOBAL_LOGGER as logger
import dbt.clients.system
import dbt.project

from dbt.task.base_task import BaseTask

PROFILE_DIR_MESSAGE = """To view your profiles.yml file, run:

{open_cmd} {profiles_dir}"""


class DebugTask(BaseTask):
    def path_info(self):
        open_cmd = dbt.clients.system.open_dir_cmd()
        profiles_dir = dbt.project.default_profiles_dir

        message = PROFILE_DIR_MESSAGE.format(
            open_cmd=open_cmd,
            profiles_dir=profiles_dir
        )

        logger.info(message)

    def diag(self):
        logger.info("args: {}".format(self.args))
        logger.info("project: ")
        pprint.pprint(self.project)

    def run(self):

        if self.args.config_dir:
            self.path_info()
        else:
            self.diag()
