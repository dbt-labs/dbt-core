import os.path

config_template = """
# This is an annotated sample project configuration for reference.
# It attempts to show all possible configuration options.

# Compile configuration
source-paths: ["models"]   # paths with source code to compile
target-path: "target"      # path for compiled code
clean-targets: ["target"]  # directories removed by the clean task

# Test configuration
test-paths: ["test"]
"""


class InitTask:
    def __init__(self, args, project):
        self.args = args
        self.project = project

    def run(self):
        "Creates a sample dbt_project.yml in the current working directory"

        config_file_path = "./dbt_project.yml"
        if not os.path.exists(config_file_path):
            with open(config_file_path, 'w') as fh:
                fh.write(config_template)
                print("wrote sample config file to {}".format(config_file_path))
        else:
            print("file '{}' already exists! Not overwriting".format(config_file_path))

