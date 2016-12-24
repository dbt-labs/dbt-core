import pprint
from dbt.logger import GLOBAL_LOGGER as logger


class DebugTask:
    def __init__(self, args, project):
        self.args = args
        self.project = project

    def run(self):
        logger.info("args: {}".format(self.args))
        logger.info("project: ")

        # TODO: switch this out for a log statement
        pprint.pprint(self.project)
