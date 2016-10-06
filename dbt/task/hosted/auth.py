import dbt.project as project
from dbt.hosted.api import DbtAPI

class HostedAuthTask:
    def __init__(self, args):
        self.args = args

    def run(self):
        api = DbtAPI()
        if api.token is not None:
            print("Already authenticatd!")
        else:
            api.authenticate()
