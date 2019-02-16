import dbt.exceptions


class BaseTask(object):
    def __init__(self, args, config=None):
        self.args = args
        self.config = config

    def run(self):
        raise dbt.exceptions.NotImplementedException('Not Implemented')

    def interpret_results(self, results):
        return True
