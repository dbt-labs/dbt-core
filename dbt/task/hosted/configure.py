import dbt.project as project
import os.path
import subprocess
import yaml
import re

from dbt.hosted.api import DbtAPI

class HostedConfigureTask:
    def __init__(self, args):
        self.args = args

    def run(self):
        api = DbtAPI()

        key = api.configure()
