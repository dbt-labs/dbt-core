from dbt.compilation import Compiler, CompilableEntities
from dbt.logger import GLOBAL_LOGGER as logger


class CompileTask:
    def __init__(self, args, project):
        self.args = args
        self.project = project

    def run(self):
        compiler = Compiler(self.project, self.args)
        compiler.initialize()
        results = compiler.compile()

        stat_line = ", ".join(
            ["{} {}s".format(ct, t) for t, ct in results.items()])

        logger.info("Compiled {}".format(stat_line))
