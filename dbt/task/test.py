
import os, sys
import psycopg2
import yaml

from dbt.compilation import Compiler, CompilableEntities
from dbt.templates import DryCreateTemplate, BaseCreateTemplate
from dbt.runner import RunManager
from dbt.schema_tester import SchemaTester

class TestTask:
    """
    Testing:
        1) Create tmp views w/ 0 rows to ensure all tables, schemas, and SQL statements are valid
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
        compiler = Compiler(self.project, BaseCreateTemplate)
        compiler.initialize()
        results = compiler.compile()

        stat_line = ", ".join(["{} {}".format(results[k], k) for k in CompilableEntities])
        print("Compiled {}".format(stat_line))

        return compiler

    def run(self):
        self.compile()
        runner = RunManager(self.project, self.project['target-path'], 'build', self.args.threads)
        runner.run_tests()

        print("Done!")
