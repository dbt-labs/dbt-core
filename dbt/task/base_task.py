import dbt.exceptions


class BaseTask(object):
    def __init__(self, args, project=None):
        self.args = args
        self.project = project

    def run(self):
        raise dbt.exceptions.NotImplementedException('Not Implemented')

    def interpret_results(self, results):
        return True


class RunnableTask(BaseTask):
    def interpret_results(self, results):
        if results is None:
            return False

        failures = [r for r in results if r.error or r.fail]
        return len(failures) == 0
