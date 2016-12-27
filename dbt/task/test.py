import os
import psycopg2
import sys
import yaml

from dbt.compilation import Compiler, CompilableEntities
from dbt.templates import DryCreateTemplate, BaseCreateTemplate
from dbt.runner import RunManager
from dbt.schema_tester import SchemaTester
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
        compiler = Compiler(self.project, BaseCreateTemplate, self.args)
        compiler.initialize()
        results = compiler.compile(limit_to=['tests'])

        stat_line = ", ".join(
            ["{} {}".format(results[k], k) for k in CompilableEntities]
        )
        logger.info("Compiled {}".format(stat_line))

        return compiler

    def run(self):
        self.compile()
        runner = RunManager(
            self.project, self.project['target-path'], 'build', self.args
        )

        if (self.args.data and self.args.schema) or \
           (not self.args.data and not self.args.schema):
            res = runner.run_tests(test_schemas=True, test_data=True)
        elif self.args.data:
            res = runner.run_tests(test_schemas=False, test_data=True)
        elif self.args.schema:
            res = runner.run_tests(test_schemas=True, test_data=False)
        else:
            raise RuntimeError("unexpected")

        logger.info("Done!")
        return res
