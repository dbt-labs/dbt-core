from pathlib import Path


def default_project_dir():
    cwd = Path.cwd()
    root_path = Path(cwd.root)

    while cwd != root_path:
        # Use the directory if there is a profiles.yml file
        if (cwd / "dbt_project.yml").exists():
            return Path(cwd)
        cwd = cwd.parent

    return Path.cwd()


def default_profiles_dir():
    default_profiles_dir = Path.home() / ".dbt"

    # Use the current working directory if there is a profiles.yml file
    if (Path.cwd() / "profiles.yml").exists():
        default_profiles_dir = Path.cwd() 

    return default_profiles_dir
