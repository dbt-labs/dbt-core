import os
from dbt.seeder import Seeder
from dbt.task.base_task import BaseTask


class SeedTask(BaseTask):
    def run(self):
        seeder = Seeder(self.project)
        seeder.seed(self.args.drop_existing)
