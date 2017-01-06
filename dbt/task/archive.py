from dbt.runner import RunManager
from dbt.templates import ArchiveInsertTemplate
from dbt.compilation import Compiler
from dbt.logger import GLOBAL_LOGGER as logger


class ArchiveTask:
    def __init__(self, args, project):
        self.args = args
        self.project = project
        self.create_template = ArchiveInsertTemplate

    def compile(self):
        compiler = Compiler(self.project, self.create_template, self.args)
        compiler.initialize()
        compiled = compiler.compile_archives()
        logger.info("Compiled {} archives".format(len(compiled)))

    def run(self):
        self.compile()

        runner = RunManager(
            self.project,
            self.project['target-path'],
            self.create_template.label,
            self.args
        )

        results = runner.run_archive()
