from dbt.runner import RunManager
from dbt.logger import GLOBAL_LOGGER as logger  # noqa


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

    def run(self):
        runner = RunManager(
            self.project, self.project['target-path'], self.args)

        include = self.args.models
        exclude = self.args.exclude

        if (self.args.data and self.args.schema) or \
           (not self.args.data and not self.args.schema):
            res = runner.run_tests(include, exclude, set())
        elif self.args.data:
            res = runner.run_tests(include, exclude, {'data'})
        elif self.args.schema:
            res = runner.run_tests(include, exclude, {'schema'})
        else:
            raise RuntimeError("unexpected")

        return res
