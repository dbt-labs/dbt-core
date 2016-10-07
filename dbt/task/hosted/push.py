import dbt.project as project

from dbt.hosted.api import DbtAPI

class HostedPushTask:
    def __init__(self, args, project):
        self.args = args
        self.project = project

    def run(self):
        api = DbtAPI()
        api.create_or_update_active_profiles(self.project)
