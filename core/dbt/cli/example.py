from dbt.cli.main import dbtRunner
from dbt.config.runtime import load_profile, load_project

if __name__ == "__main__":
    project_dir = "/Users/chenyuli/git/jaffle_shop"
    cli_args = ["run", "--project-dir", project_dir]

    # initialize the dbt runner
    dbt = dbtRunner()
    # run the command
    res, success = dbt.invoke(cli_args)

    # preload profile and project
    profile = load_profile(project_dir, {}, "testing-postgres")
    project = load_project(project_dir, False, profile, {})

    # initialize the runner with pre-loaded profile and project, you can also pass in a preloaded manifest
    dbt = dbtRunner(profile=profile, project=project)
    # run the command, this will use the pre-loaded profile and project instead of loading
    res, success = dbt.invoke(cli_args)
