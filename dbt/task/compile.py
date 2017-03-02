import dbt.compilation

from dbt.logger import GLOBAL_LOGGER as logger


class CompileTask:
    def __init__(self, args, project):
        self.args = args
        self.project = project

    def run(self):
        dbt.compilation.compile_and_print_status(
            self.project, self.args)
