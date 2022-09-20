import os
import dbt.exceptions

from pathlib import Path


# TODO  Duplicated code with minimal changes from get_nearest_project_dir() within core/dbt/task/base.py
# Uses os.path when it could be upgraded to Path instead
# Decide whether to retain the error-raising portion of get_nearest_project_dir()
# Uses os.path when it could be upgraded to Path instead
def get_nearest_project_dir(suppress_exception=False):
    root_path = os.path.abspath(os.sep)
    cwd = os.getcwd()

    while cwd != root_path:
        project_file = os.path.join(cwd, "dbt_project.yml")
        if os.path.exists(project_file):
            return Path(cwd)
        cwd = os.path.dirname(cwd)

    if not suppress_exception:
        raise dbt.exceptions.RuntimeException(
            "fatal: Not a dbt project (or any of the parent directories). "
            "Missing dbt_project.yml file"
        )

    return Path.cwd()


def default_profiles_dir():
    default_profiles_dir = Path.home() / ".dbt"

    # Use the current working directory if there is a profiles.yml file
    if (Path.cwd() / "profiles.yml").exists():
        default_profiles_dir = Path.cwd() 

    return default_profiles_dir
