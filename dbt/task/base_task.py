import dbt.exceptions


class BaseTask(object):
    def __init__(self, args, project=None):
        self.args = args
        self.project = project

    def run(self):
        raise dbt.exceptions.NotImplementedException('Not Implemented')

    def interpret_results(self, results):
        return True

    def run_and_get_status(self):
        results = self.run()
        return self.results_indicate_success(results):
