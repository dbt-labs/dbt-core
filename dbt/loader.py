
import os

GLOBAL_PROJECT_DIRNAME = 'global_project'


def get_dbt_root_path():
    "return the root path of the dbt module"

    try:
        here = __file__

        if os.path.islink(here):
            here = os.path.realpath(here)

        path = os.path.dirname(os.path.abspath(here))
        dbt_root_path = os.path.normpath(os.path.join(path, '..'))

    except Exception as e:
        raise RuntimeError("Couldn't determine include path")

    return dbt_root_path


def get_include_path():
    "return the static include path for the dbt module"

    root_path = get_dbt_root_path()
    return os.path.join(root_path, "include")


def get_global_project_path():
    "get the path to the actual global project"
    include_path = get_include_path()
    return os.path.join(include_path, GLOBAL_PROJECT_DIRNAME)
