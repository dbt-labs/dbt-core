import os
import pytest  # type: ignore
import random
from argparse import Namespace
from datetime import datetime
import yaml

import dbt.flags as flags
from dbt.config.runtime import RuntimeConfig
from dbt.adapters.factory import get_adapter, register_adapter, reset_adapters
from dbt.events.functions import setup_event_logger
from dbt.tests.util import write_file, run_sql_with_adapter


# These are the fixtures that are used in dbt core functional tests

# Used in constructing the unique_schema and logs_dir
@pytest.fixture(scope="class")
def prefix():
    # create a directory name that will be unique per test session
    _randint = random.randint(0, 9999)
    _runtime_timedelta = datetime.utcnow() - datetime(1970, 1, 1, 0, 0, 0)
    _runtime = (int(_runtime_timedelta.total_seconds() * 1e6)) + _runtime_timedelta.microseconds
    prefix = f"test{_runtime}{_randint:04}"
    return prefix


# Every test has a unique schema
@pytest.fixture(scope="class")
def unique_schema(request, prefix) -> str:
    test_file = request.module.__name__
    # We only want the last part of the name
    test_file = test_file.split(".")[-1]
    unique_schema = f"{prefix}_{test_file}"
    return unique_schema


# Create a directory for the profile using tmpdir fixture
@pytest.fixture(scope="class")
def profiles_root(tmpdir_factory):
    return tmpdir_factory.mktemp("profile")


# Create a directory for the project using tmpdir fixture
@pytest.fixture(scope="class")
def project_root(tmpdir_factory):
    # tmpdir docs - https://docs.pytest.org/en/6.2.x/tmpdir.html
    project_root = tmpdir_factory.mktemp("project")
    print(f"\n=== Test project_root: {project_root}")
    return project_root


# This is for data used by multiple tests, in the 'tests/data' directory
@pytest.fixture(scope="session")
def shared_data_dir(request):
    return os.path.join(request.config.rootdir, "tests", "data")


# This is for data for a specific test directory, i.e. tests/basic/data
@pytest.fixture(scope="module")
def test_data_dir(request):
    return os.path.join(request.fspath.dirname, "data")


# The profile dictionary, used to write out profiles.yml
@pytest.fixture(scope="class")
def dbt_profile_data(unique_schema):
    profile = {
        "config": {"send_anonymous_usage_stats": False},
        "test": {
            "outputs": {
                "default": {
                    "type": "postgres",
                    "threads": 4,
                    "host": "localhost",
                    "port": int(os.getenv("POSTGRES_TEST_PORT", 5432)),
                    "user": os.getenv("POSTGRES_TEST_USER", "root"),
                    "pass": os.getenv("POSTGRES_TEST_PASS", "password"),
                    "dbname": os.getenv("POSTGRES_TEST_DATABASE", "dbt"),
                    "schema": unique_schema,
                },
                "other_schema": {
                    "type": "postgres",
                    "threads": 4,
                    "host": "localhost",
                    "port": int(os.getenv("POSTGRES_TEST_PORT", 5432)),
                    "user": "noaccess",
                    "pass": "password",
                    "dbname": os.getenv("POSTGRES_TEST_DATABASE", "dbt"),
                    "schema": unique_schema + "_alt",  # Should this be the same unique_schema?
                },
            },
            "target": "default",
        },
    }
    return profile


# Write out the profile data as a yaml file
@pytest.fixture(scope="class")
def profiles_yml(profiles_root, dbt_profile_data):
    os.environ["DBT_PROFILES_DIR"] = str(profiles_root)
    write_file(yaml.safe_dump(dbt_profile_data), profiles_root, "profiles.yml")
    yield dbt_profile_data
    del os.environ["DBT_PROFILES_DIR"]


# This fixture can be overridden in a project
@pytest.fixture(scope="class")
def project_config_update():
    return {}


# Combines the project_config_update dictionary with defaults to
# produce a project_yml config and write it out as dbt_project.yml
@pytest.fixture(scope="class")
def dbt_project_yml(project_root, project_config_update, logs_dir):
    project_config = {
        "config-version": 2,
        "name": "test",
        "version": "0.1.0",
        "profile": "test",
        "log-path": logs_dir,
    }
    if project_config_update:
        project_config.update(project_config_update)
    write_file(yaml.safe_dump(project_config), project_root, "dbt_project.yml")


# Fixture to provide packages as either yaml or dictionary
@pytest.fixture(scope="class")
def packages():
    return {}


# Write out the packages.yml file
@pytest.fixture(scope="class")
def packages_yml(project_root, packages):
    if packages:
        if isinstance(packages, str):
            data = packages
        else:
            data = yaml.safe_dump(packages)
        write_file(data, project_root, "packages.yml")


# Fixture to provide selectors as either yaml or dictionary
@pytest.fixture(scope="class")
def selectors():
    return {}


# Write out the selectors.yml file
@pytest.fixture(scope="class")
def selectors_yml(project_root, selectors):
    if selectors:
        if isinstance(selectors, str):
            data = selectors
        else:
            data = yaml.safe_dump(selectors)
        write_file(data, project_root, "selectors.yml")


# This creates an adapter that is used for running test setup and teardown,
# and 'run_sql' commands. The 'run_dbt' commands will create their own adapter
# so this one needs some special patching to run after dbt commands have been
# executed
@pytest.fixture(scope="class")
def adapter(unique_schema, project_root, profiles_root, profiles_yml, dbt_project_yml):
    # The profiles.yml and dbt_project.yml should already be written out
    args = Namespace(
        profiles_dir=str(profiles_root), project_dir=str(project_root), target=None, profile=None
    )
    flags.set_from_args(args, {})
    runtime_config = RuntimeConfig.from_args(args)
    register_adapter(runtime_config)
    adapter = get_adapter(runtime_config)
    yield adapter
    adapter.cleanup_connections()
    reset_adapters()


# Start at directory level.
def write_project_files(project_root, dir_name, file_dict):
    path = project_root.mkdir(dir_name)
    if file_dict:
        write_project_files_recursively(path, file_dict)


# Write files out from file_dict. Can be nested directories...
def write_project_files_recursively(path, file_dict):
    for name, value in file_dict.items():
        if name.endswith(".sql") or name.endswith(".csv") or name.endswith(".md"):
            write_file(value, path, name)
        elif name.endswith(".yml") or name.endswith(".yaml"):
            if isinstance(value, str):
                data = value
            else:
                data = yaml.safe_dump(value)
            write_file(data, path, name)
        else:
            write_project_files_recursively(path.mkdir(name), value)


# models, macros, seeds, snapshots, tests, analysis
# Provide a dictionary of file names to contents. Nested directories
# are handle by nested dictionaries.
@pytest.fixture(scope="class")
def models():
    return {}


@pytest.fixture(scope="class")
def macros():
    return {}


@pytest.fixture(scope="class")
def seeds():
    return {}


@pytest.fixture(scope="class")
def snapshots():
    return {}


@pytest.fixture(scope="class")
def tests():
    return {}


@pytest.fixture(scope="class")
def analysis():
    return {}


# Write out the files provided by models, macros, snapshots, seeds, tests, analysis
@pytest.fixture(scope="class")
def project_files(project_root, models, macros, snapshots, seeds, tests, analysis):
    write_project_files(project_root, "models", models)
    write_project_files(project_root, "macros", macros)
    write_project_files(project_root, "snapshots", snapshots)
    write_project_files(project_root, "seeds", seeds)
    write_project_files(project_root, "tests", tests)
    write_project_files(project_root, "analysis", analysis)


# We have a separate logs dir for every test
@pytest.fixture(scope="class")
def logs_dir(request, prefix):
    return os.path.join(request.config.rootdir, "logs", prefix)


# This class is returned from the 'project' fixture, and contains information
# from the pytest fixtures that may be needed in the test functions, including
# a 'run_sql' method.
class TestProjInfo:
    def __init__(
        self,
        project_root,
        profiles_dir,
        adapter,
        test_dir,
        shared_data_dir,
        test_data_dir,
        test_schema,
        database,
    ):
        self.project_root = project_root
        self.profiles_dir = profiles_dir
        self.adapter = adapter
        self.test_dir = test_dir
        self.shared_data_dir = shared_data_dir
        self.test_data_dir = test_data_dir
        self.test_schema = test_schema
        self.database = database

    # Run sql from a path
    def run_sql_file(self, sql_path, fetch=None):
        with open(sql_path, "r") as f:
            statements = f.read().split(";")
            for statement in statements:
                self.run_sql(statement, fetch)

    # run sql from a string, using adapter saved at test startup
    def run_sql(self, sql, fetch=None):
        return run_sql_with_adapter(self.adapter, sql, fetch=fetch)

    def get_tables_in_schema(self):
        sql = """
                select table_name,
                        case when table_type = 'BASE TABLE' then 'table'
                             when table_type = 'VIEW' then 'view'
                             else table_type
                        end as materialization
                from information_schema.tables
                where {}
                order by table_name
                """
        sql = sql.format("{} ilike '{}'".format("table_schema", self.test_schema))
        result = self.run_sql(sql, fetch="all")
        return {model_name: materialization for (model_name, materialization) in result}


@pytest.fixture(scope="class")
def project(
    project_root,
    profiles_root,
    request,
    unique_schema,
    profiles_yml,
    dbt_project_yml,
    packages_yml,
    selectors_yml,
    adapter,
    project_files,
    shared_data_dir,
    test_data_dir,
    logs_dir,
):
    setup_event_logger(logs_dir)
    orig_cwd = os.getcwd()
    os.chdir(project_root)
    # Return whatever is needed later in tests but can only come from fixtures, so we can keep
    # the signatures in the test signature to a minimum.
    project = TestProjInfo(
        project_root=project_root,
        profiles_dir=profiles_root,
        adapter=adapter,
        test_dir=request.fspath.dirname,
        shared_data_dir=shared_data_dir,
        test_data_dir=test_data_dir,
        test_schema=unique_schema,
        # the following feels kind of fragile. TODO: better way of getting database
        database=profiles_yml["test"]["outputs"]["default"]["dbname"],
    )
    project.run_sql("drop schema if exists {schema} cascade")
    project.run_sql("create schema {schema}")

    yield project

    project.run_sql("drop schema if exists {schema} cascade")
    os.chdir(orig_cwd)
