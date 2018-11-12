import os.path
import os
import shutil

from dbt.task.base_task import BaseTask


class CleanTask(BaseTask):

    def __is_project_path(self, path):
        proj_path = os.path.abspath('.')
        return not os.path.commonprefix(
            [proj_path, os.path.abspath(path)]
        ) == proj_path

    def __is_protected_path(self, path):
        abs_path = os.path.abspath(path)
        protected_paths = self.config.source_paths + \
            self.config.test_paths + ['.']

        protected_abs_paths = [os.path.abspath for p in protected_paths]
        return abs_path in set(protected_abs_paths) or \
            self.__is_project_path(abs_path)

    def run(self):
        for path in self.config.clean_targets:
            if not self.__is_protected_path(path):
                shutil.rmtree(path, True)
