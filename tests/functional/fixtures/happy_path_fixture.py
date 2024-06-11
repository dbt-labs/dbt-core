import os
from distutils.dir_util import copy_tree

import pytest


def delete_files_in_directory(directory_path):
    try:
        with os.scandir(directory_path) as entries:
            for entry in entries:
                if entry.is_file():
                    os.unlink(entry.path)
        print("All files deleted successfully.")
    except OSError:
        print("Error occurred while deleting files.")


@pytest.fixture(scope="class")
def happy_path_project_files(project_root):
    # copy fixture files to the project root
    delete_files_in_directory(project_root)
    copy_tree(
        os.path.dirname(os.path.realpath(__file__)) + "/happy_path_project", str(project_root)
    )


@pytest.fixture(scope="class")
def happy_path_project(project_setup, happy_path_project_files):
    return project_setup
