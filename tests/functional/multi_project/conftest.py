import pytest
import yaml
from dbt.tests.util import write_file
from dbt.tests.fixtures.project import write_project_files_recursively

# This fixture should always run after the standard "project" fixture, because
# it skips a lot of setup with the assumption that the "project" fixture has done it.
# In particular, you can't execute sql or do other things on this fixture. It uses
# the same unique schema as the base project fixture.


@pytest.fixture(scope="class")
def project_root_alt(tmpdir_factory):
    # tmpdir docs - https://docs.pytest.org/en/6.2.x/tmpdir.html
    project_root = tmpdir_factory.mktemp("project")
    print(f"\n=== Test project_root alt: {project_root}")
    return project_root


# Data used to update the dbt_project config data.
@pytest.fixture(scope="class")
def project_config_update_alt():
    return {}


# Combines the project_config_update dictionary with project_config defaults to
# produce a project_yml config and write it out as dbt_project.yml
@pytest.fixture(scope="class")
def dbt_project_yml_alt(project_root_alt, project_config_update_alt):
    project_config = {
        "name": "test_alt",
        "profile": "test",
    }
    if project_config_update_alt:
        if isinstance(project_config_update_alt, dict):
            project_config.update(project_config_update_alt)
        elif isinstance(project_config_update_alt, str):
            updates = yaml.safe_load(project_config_update_alt)
            project_config.update(updates)
    write_file(yaml.safe_dump(project_config), project_root_alt, "dbt_project.yml")
    return project_config


def write_project_files_alt(project_root_alt, dir_name, file_dict):
    path = project_root_alt.mkdir(dir_name)
    if file_dict:
        write_project_files_recursively(path, file_dict)


@pytest.fixture(scope="class")
def models_alt():
    return {}


@pytest.fixture(scope="class")
def project_files_alt(project_root_alt, models_alt):
    write_project_files_alt(project_root_alt, "models", {**models_alt})


class TestProjInfoAlt:
    def __init__(
        self,
        project_root_alt,
    ):
        self.project_root = project_root_alt


@pytest.fixture(scope="class")
def project_alt(
    project_root_alt,
    dbt_project_yml_alt,
    project_files_alt,
):
    project_alt = TestProjInfoAlt(
        project_root_alt=project_root_alt,
    )

    yield project_alt
