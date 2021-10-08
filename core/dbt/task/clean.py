import os.path
import os
import shutil

from dbt import deprecations
from dbt.task.base import BaseTask, move_to_nearest_project_dir
from dbt.logger import GLOBAL_LOGGER as logger
from dbt.config import UnsetProfileConfig


class CleanTask(BaseTask):
    ConfigType = UnsetProfileConfig

    def __is_project_path(self, path):
        proj_path = os.path.abspath('.')
        return not os.path.commonprefix(
            [proj_path, os.path.abspath(path)]
        ) == proj_path

    def __is_protected_path(self, path):
        """
        This function identifies protected paths, so as not to clean them.
        """
        abs_path = os.path.abspath(path)
        protected_paths = self.config.model_paths + \
            self.config.test_paths + ['.']
        protected_abs_paths = [os.path.abspath(p) for p in protected_paths]
        return abs_path in set(protected_abs_paths) or \
            self.__is_project_path(abs_path)

    def run(self):
        """
        This function takes all the paths in the target file
        and cleans the project paths that are not protected.
        """
        move_to_nearest_project_dir(self.args)
        if ('dbt_modules' in self.config.clean_targets and
           self.config.packages_install_path != 'dbt_modules'):
            deprecations.warn('install-packages-path')
        for path in self.config.clean_targets:
            logger.info("Checking {}/*".format(path))
            if not self.__is_protected_path(path):
                shutil.rmtree(path, True)
                logger.info(" Cleaned {}/*".format(path))
            else:
                logger.info("ERROR: not cleaning {}/* because it is "
                            "protected".format(path))
        logger.info("Finished cleaning all paths.")
