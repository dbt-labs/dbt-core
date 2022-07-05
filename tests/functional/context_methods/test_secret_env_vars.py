import pytest
import os

from dbt.tests.util import run_dbt, run_dbt_and_capture
from dbt.exceptions import ParsingException, InternalException
from dbt.logger import SECRET_ENV_PREFIX
from tests.functional.context_methods.first_dependency import FirstDependencyProject


secret_bad__context_sql = """

{{
    config(
        materialized='table'
    )
}}

select

    '{{ env_var("DBT_TEST_ENV_VAR") }}' as env_var,
    '{{ env_var("DBT_ENV_SECRET_SECRET") }}' as env_var_secret, -- this should raise an error!
    '{{ env_var("DBT_TEST_NOT_SECRET") }}' as env_var_not_secret

"""


class TestDisallowSecretModel:
    @pytest.fixture(scope="class")
    def models(self):
        return {"context.sql": secret_bad__context_sql}

    def test_disallow_secret(self, project):
        with pytest.raises(ParsingException):
            run_dbt(["compile"])


models__context_sql = """
{{
    config(
        materialized='table'
    )
}}

select

    -- compile-time variables
    '{{ this }}'        as "this",
    '{{ this.name }}'   as "this.name",
    '{{ this.schema }}' as "this.schema",
    '{{ this.table }}'  as "this.table",

    '{{ target.dbname }}'  as "target.dbname",
    '{{ target.host }}'    as "target.host",
    '{{ target.name }}'    as "target.name",
    '{{ target.schema }}'  as "target.schema",
    '{{ target.type }}'    as "target.type",
    '{{ target.user }}'    as "target.user",
    '{{ target.get("pass", "") }}'    as "target.pass", -- not actually included, here to test that it is _not_ present!
    {{ target.port }}      as "target.port",
    {{ target.threads }}   as "target.threads",

    -- runtime variables
    '{{ run_started_at }}' as run_started_at,
    '{{ invocation_id }}'  as invocation_id,

    '{{ env_var("DBT_TEST_ENV_VAR") }}' as env_var,
    'secret_variable' as env_var_secret, -- make sure the value itself is scrubbed from the logs
    '{{ env_var("DBT_TEST_NOT_SECRET") }}' as env_var_not_secret
"""


class TestAllowSecretProfilePackage(FirstDependencyProject):
    @pytest.fixture(scope="class", autouse=True)
    def setup(self):
        os.environ[SECRET_ENV_PREFIX + "USER"] = "root"
        os.environ[SECRET_ENV_PREFIX + "PASS"] = "password"
        os.environ[SECRET_ENV_PREFIX + "PACKAGE"] = "first_dependency"
        yield
        del os.environ[SECRET_ENV_PREFIX + "USER"]
        del os.environ[SECRET_ENV_PREFIX + "PASS"]
        del os.environ[SECRET_ENV_PREFIX + "PACKAGE"]

    @pytest.fixture(scope="class")
    def models(self):
        return {"context.sql": models__context_sql}

    @pytest.fixture(scope="class")
    def packages(self):
        return {
            "packages": [
                {"local": "{{ env_var('DBT_ENV_SECRET_PACKAGE') }}"},
            ]
        }

    @pytest.fixture(scope="class")
    def profile_target(self):
        return {
            "type": "postgres",
            "threads": 1,
            "host": "localhost",
            "port": 5432,
            # root/password
            "user": "{{ env_var('DBT_ENV_SECRET_USER') }}",
            "pass": "{{ env_var('DBT_ENV_SECRET_PASS') }}",
            "dbname": "dbt",
        }

    def test_allow_secrets(self, project, first_dependency):
        _, log_output = run_dbt_and_capture(["deps"])
        assert not ("first_dependency" in log_output)


class TestCloneFailSecretScrubbed:
    @pytest.fixture(scope="class", autouse=True)
    def setup(self):
        os.environ[SECRET_ENV_PREFIX + "GIT_TOKEN"] = "abc123"

    @pytest.fixture(scope="class")
    def models(self):
        return {"context.sql": models__context_sql}

    @pytest.fixture(scope="class")
    def packages(self):
        return {
            "packages": [
                {
                    "git": "https://fakeuser:{{ env_var('DBT_ENV_SECRET_GIT_TOKEN') }}@github.com/dbt-labs/fake-repo.git"
                },
            ]
        }

    def test_fail_clone_with_scrubbing(self, project):
        with pytest.raises(InternalException) as excinfo:
            _, log_output = run_dbt_and_capture(["deps"])

        assert "abc123" not in str(excinfo.value)


class TestCloneFailSecretNotRendered(TestCloneFailSecretScrubbed):
    # as above, with some Jinja manipulation
    @pytest.fixture(scope="class")
    def packages(self):
        return {
            "packages": [
                {
                    "git": "https://fakeuser:{{ env_var('DBT_ENV_SECRET_GIT_TOKEN') | join(' ') }}@github.com/dbt-labs/fake-repo.git"
                },
            ]
        }

    def test_fail_clone_with_scrubbing(self, project):
        with pytest.raises(InternalException) as excinfo:
            _, log_output = run_dbt_and_capture(["deps"])

        # we should not see any manipulated form of the secret value (abc123) here
        # we should see a manipulated form of the placeholder instead
        assert "a b c 1 2 3" not in str(excinfo.value)
        assert "D B T _ E N V _ S E C R E T _ G I T _ T O K E N" in str(excinfo.value)
