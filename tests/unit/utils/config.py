import pytest

from dbt.adapters.postgres.connections import PostgresCredentials
from dbt.config.profile import Profile
from dbt.config.project import Project
from dbt.config.renderer import ProfileRenderer
from dbt.config.runtime import RuntimeConfig


@pytest.fixture
def credentials() -> PostgresCredentials:
    return PostgresCredentials(
        database="test_database",
        schema="test_schema",
        host="test_host",
        user="test_user",
        port=1337,
        password="test_password",
    )


@pytest.fixture
def profile() -> Profile:
    profile_yaml = {
        "target": "postgres",
        "outputs": {
            "postgres": {
                "type": "postgres",
                "host": "postgres-db-hostname",
                "port": 5555,
                "user": "db_user",
                "pass": "db_pass",
                "dbname": "postgres-db-name",
                "schema": "postgres-schema",
                "threads": 7,
            },
        },
    }
    return Profile.from_raw_profile_info(
        raw_profile=profile_yaml, profile_name="test_profile", renderer=ProfileRenderer({})
    )


@pytest.fixture
def runtime_config(project: Project, profile: Profile) -> RuntimeConfig:
    return RuntimeConfig.from_parts(
        project=project,
        profile=profile,
        args={},
    )
