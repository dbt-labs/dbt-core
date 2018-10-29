import pprint

from dbt.logger import GLOBAL_LOGGER as logger
import dbt.clients.system
import dbt.config
import dbt.utils
import dbt.exceptions

from dbt.task.base_task import BaseTask

PROFILE_DIR_MESSAGE = """To view your profiles.yml file, run:

{open_cmd} {profiles_dir}"""


class DebugTask(BaseTask):
    def path_info(self):
        open_cmd = dbt.clients.system.open_dir_cmd()
        profiles_dir = dbt.config.PROFILES_DIR

        message = PROFILE_DIR_MESSAGE.format(
            open_cmd=open_cmd,
            profiles_dir=profiles_dir
        )

        logger.info(message)

    def diag(self):
        # if we got here, a 'dbt_project.yml' does exist, but we have not tried
        # to parse it.
        project_profile = None
        cli_vars = dbt.utils.parse_cli_vars(getattr(self.args, 'vars', '{}'))

        try:
            project = dbt.config.Project.from_current_directory(cli_vars)
            project_profile = project.profile_name
        except dbt.exceptions.DbtConfigError as exc:
            project = 'ERROR loading project: {!s}'.format(exc)

        # log the profile we decided on as well, if it's available.
        try:
            profile = dbt.config.Profile.from_args(self.args, project_profile,
                                                   cli_vars)
        except dbt.exceptions.DbtConfigError as exc:
            profile = 'ERROR loading profile: {!s}'.format(exc)

        logger.info("args: {}".format(self.args))
        logger.info("")
        logger.info("project:\n{!s}".format(project))
        logger.info("")
        logger.info("profile:\n{!s}".format(profile))

    def run(self):

        if self.args.config_dir:
            self.path_info()
        else:
            self.diag()
