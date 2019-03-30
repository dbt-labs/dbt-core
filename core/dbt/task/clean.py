import os.path
import os
import shutil

from dbt.task.base_task import BaseTask
from dbt.logger import GLOBAL_LOGGER as logger


class CleanTask(BaseTask):

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
        protected_paths = self.config.source_paths + \
            self.config.test_paths + ['.']
        protected_abs_paths = [os.path.abspath for p in protected_paths]
        return abs_path in set(protected_abs_paths) or \
            self.__is_project_path(abs_path)

    def run(self):
        """
        This function takes all the paths in the target file
        and cleans the project paths that are not protected.
        """
        for path in self.config.clean_targets:
            logger.info("Checking {}/*".format(path))
            if not self.__is_protected_path(path):
                shutil.rmtree(path, True)
                logger.info(" Cleaned {}/*".format(path))
        logger.info("Finished cleaning all paths.")
