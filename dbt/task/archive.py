from dbt.runner import RunManager
from dbt.compilation import Compiler
from dbt.logger import GLOBAL_LOGGER as logger


class ArchiveTask:
    def __init__(self, args, project):
        self.args = args
        self.project = project

    def compile(self):
        compiler = Compiler(self.project, self.args)
        compiler.initialize()
        results = compiler.compile()

        stat_line = ", ".join(
            ["{} {}s".format(ct, t) for t, ct in results.items()])

        logger.info("Compiled {}".format(stat_line))

    def run(self):
        self.compile()

        runner = RunManager(
            self.project,
            self.project['target-path'],
            self.args
        )

        runner.run_archives(['*'], [])
