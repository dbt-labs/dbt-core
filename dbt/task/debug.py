import pprint


class DebugTask:
    def __init__(self, args, project):
        self.args = args
        self.project = project

    def run(self):
        logger.info("args: {}".format(self.args))
        logger.info("project: ")
        pprint.pprint(self.project)
