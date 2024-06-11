import os
import shutil

import pytest


@pytest.fixture(scope="class")
def happy_path_project_files(project_root):
    # copy fixture files to the project root
    shutil.rmtree(project_root)
    shutil.copytree(
        os.path.dirname(os.path.realpath(__file__)) + "/happy_path_project", project_root
    )


@pytest.fixture(scope="class")
def happy_path_project(project_setup, happy_path_project_files):
    return project_setup
