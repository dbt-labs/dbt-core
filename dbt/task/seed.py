import os
from dbt.seeder import Seeder


class SeedTask:
    def __init__(self, args, project):
        self.args = args
        self.project = project

    def run(self):
        seeder = Seeder(self.project)
        seeder.seed(self.args.drop_existing)
