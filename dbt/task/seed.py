import os
from dbt.seeder import Seeder
from dbt.task.base_task import BaseTask


class SeedTask(BaseTask):
    def run(self):
        seeder = Seeder(self.project)
        self.success = seeder.seed(self.args.drop_existing)

    def interpret_results(self, results):
        return self.success
