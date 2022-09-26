from pathlib import Path


def default_project_dir():
    paths = list(Path.cwd().parents)
    paths.insert(0, Path.cwd())
    return next((x for x in paths if (x / "dbt_project.yml").exists()), Path.cwd())


def default_profiles_dir():
    return Path.cwd() if (Path.cwd() / "profiles.yml").exists() else Path.home() / ".dbt"
