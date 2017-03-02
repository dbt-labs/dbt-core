
from dbt.compilation import Compiler, CompilableEntities
from dbt.runner import RunManager
from dbt.logger import GLOBAL_LOGGER as logger


class TestTask:
    """
    Testing:
        1) Create tmp views w/ 0 rows to ensure all tables, schemas, and SQL
           statements are valid
        2) Read schema files and validate that constraints are satisfied
           a) not null
           b) uniquenss
           c) referential integrity
           d) accepted value
    """
    def __init__(self, args, project):
        self.args = args
        self.project = project

    def compile(self):
        compiler = Compiler(self.project, self.args)
        compiler.initialize()
        results = compiler.compile()

        stat_line = ", ".join(
            ["{} {}".format(results[k], k) for k in CompilableEntities]
        )
        logger.info("Compiled {}".format(stat_line))

    def run(self):
        self.compile()

        runner = RunManager(
            self.project, self.project['target-path'], self.args
        )

        include = self.args.models
        exclude = self.args.exclude

        if (self.args.data and self.args.schema) or \
           (not self.args.data and not self.args.schema):
            res = runner.run_tests(include, exclude,
                                   test_schemas=True, test_data=True)
        elif self.args.data:
            res = runner.run_tests(include, exclude,
                                   test_schemas=False, test_data=True)
        elif self.args.schema:
            res = runner.run_tests(include, exclude,
                                   test_schemas=True, test_data=False)
        else:
            raise RuntimeError("unexpected")

        logger.info("Done!")
        return res
