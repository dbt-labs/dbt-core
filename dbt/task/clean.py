import os.path
import os
import shutil


class CleanTask:

    def __init__(self, args, project):
        self.args = args
        self.project = project

    def __is_project_path(self, path):
        proj_path = os.path.abspath('.')
        return not os.path.commonprefix(
            [proj_path, os.path.abspath(path)]
        ) == proj_path

    def __is_protected_path(self, path):
        abs_path = os.path.abspath(path)
        protected_paths = self.project['source-paths'] + \
            self.project['test-paths'] + ['.']

        protected_abs_paths = [os.path.abspath for p in protected_paths]
        return abs_path in set(protected_abs_paths) or \
            self.__is_project_path(abs_path)

    def run(self):
        for path in self.project['clean-targets']:
            if not self.__is_protected_path(path):
                shutil.rmtree(path, True)
