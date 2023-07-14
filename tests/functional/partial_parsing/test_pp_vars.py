import os
from pathlib import Path

import pytest
from dbt.constants import SECRET_ENV_PREFIX
from dbt.exceptions import FailedToConnectError, ParsingError
from dbt.tests.util import get_manifest, run_dbt, run_dbt_and_capture, write_file

from tests.functional.partial_parsing.fixtures import (
    env_var_macro_sql,
    env_var_macros_yml,
    env_var_metrics_yml,
    env_var_model_one_sql,
    env_var_model_sql,
    env_var_model_test_yml,
    env_var_schema2_yml,
    env_var_schema3_yml,
    env_var_schema_yml,
    env_var_sources_yml,
    metricflow_time_spine_sql,
    model_color_sql,
    model_one_sql,
    people_semantic_models_yml,
    people_sql,
    raw_customers_csv,
    test_color_sql,
)

os.environ["DBT_PP_TEST"] = "true"


class TestEnvVars:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "model_color.sql": model_color_sql,
        }

    def test_env_vars_models(self, project):

        # initial run
        results = run_dbt(["run"])
        assert len(results) == 1

        # copy a file with an env_var call without an env_var
        write_file(env_var_model_sql, project.project_root, "models", "env_var_model.sql")
        with pytest.raises(ParsingError):
            results = run_dbt(["--partial-parse", "run"])

        # set the env var
        os.environ["ENV_VAR_TEST"] = "TestingEnvVars"
        results = run_dbt(["--partial-parse", "run"])
        assert len(results) == 2
        manifest = get_manifest(project.project_root)
        expected_env_vars = {"ENV_VAR_TEST": "TestingEnvVars"}
        assert expected_env_vars == manifest.env_vars
        model_id = "model.test.env_var_model"
        model = manifest.nodes[model_id]
        model_created_at = model.created_at

        # change the env var
        os.environ["ENV_VAR_TEST"] = "second"
        results = run_dbt(["--partial-parse", "run"])
        assert len(results) == 2
        manifest = get_manifest(project.project_root)
        expected_env_vars = {"ENV_VAR_TEST": "second"}
        assert expected_env_vars == manifest.env_vars
        assert model_created_at != manifest.nodes[model_id].created_at

        # set an env_var in a schema file
        write_file(env_var_schema_yml, project.project_root, "models", "schema.yml")
        write_file(env_var_model_one_sql, project.project_root, "models", "model_one.sql")
        with pytest.raises(ParsingError):
            results = run_dbt(["--partial-parse", "run"])

        # actually set the env_var
        os.environ["TEST_SCHEMA_VAR"] = "view"
        results = run_dbt(["--partial-parse", "run"])
        manifest = get_manifest(project.project_root)
        expected_env_vars = {"ENV_VAR_TEST": "second", "TEST_SCHEMA_VAR": "view"}
        assert expected_env_vars == manifest.env_vars

        # env vars in a source
        os.environ["ENV_VAR_DATABASE"] = "dbt"
        os.environ["ENV_VAR_SEVERITY"] = "warn"
        write_file(raw_customers_csv, project.project_root, "seeds", "raw_customers.csv")
        write_file(env_var_sources_yml, project.project_root, "models", "sources.yml")
        run_dbt(["--partial-parse", "seed"])
        results = run_dbt(["--partial-parse", "run"])
        assert len(results) == 3
        manifest = get_manifest(project.project_root)
        expected_env_vars = {
            "ENV_VAR_TEST": "second",
            "TEST_SCHEMA_VAR": "view",
            "ENV_VAR_DATABASE": "dbt",
            "ENV_VAR_SEVERITY": "warn",
        }
        assert expected_env_vars == manifest.env_vars
        assert len(manifest.sources) == 1
        source_id = "source.test.seed_sources.raw_customers"
        source = manifest.sources[source_id]
        assert source.database == "dbt"
        schema_file = manifest.files[source.file_id]
        test_id = "test.test.source_not_null_seed_sources_raw_customers_id.e39ee7bf0d"
        test_node = manifest.nodes[test_id]
        assert test_node.config.severity == "WARN"

        # Change severity env var
        os.environ["ENV_VAR_SEVERITY"] = "error"
        results = run_dbt(["--partial-parse", "run"])
        manifest = get_manifest(project.project_root)
        expected_env_vars = {
            "ENV_VAR_TEST": "second",
            "TEST_SCHEMA_VAR": "view",
            "ENV_VAR_DATABASE": "dbt",
            "ENV_VAR_SEVERITY": "error",
        }
        assert expected_env_vars == manifest.env_vars
        source_id = "source.test.seed_sources.raw_customers"
        source = manifest.sources[source_id]
        schema_file = manifest.files[source.file_id]
        expected_schema_file_env_vars = {
            "sources": {"seed_sources": ["ENV_VAR_DATABASE", "ENV_VAR_SEVERITY"]}
        }
        assert expected_schema_file_env_vars == schema_file.env_vars
        test_node = manifest.nodes[test_id]
        assert test_node.config.severity == "ERROR"

        # Change database env var
        os.environ["ENV_VAR_DATABASE"] = "test_dbt"
        results = run_dbt(["--partial-parse", "run"])
        manifest = get_manifest(project.project_root)
        expected_env_vars = {
            "ENV_VAR_TEST": "second",
            "TEST_SCHEMA_VAR": "view",
            "ENV_VAR_DATABASE": "test_dbt",
            "ENV_VAR_SEVERITY": "error",
        }
        assert expected_env_vars == manifest.env_vars
        source = manifest.sources[source_id]
        assert source.database == "test_dbt"

        # Delete database env var
        del os.environ["ENV_VAR_DATABASE"]
        with pytest.raises(ParsingError):
            results = run_dbt(["--partial-parse", "run"])
        os.environ["ENV_VAR_DATABASE"] = "test_dbt"

        # Add generic test with test kwarg that's rendered late (no curly brackets)
        os.environ["ENV_VAR_DATABASE"] = "dbt"
        write_file(test_color_sql, project.project_root, "macros", "test_color.sql")
        results = run_dbt(["--partial-parse", "run"])
        # Add source test using test_color and an env_var for color
        write_file(env_var_schema2_yml, project.project_root, "models/schema.yml")
        with pytest.raises(ParsingError):
            results = run_dbt(["--partial-parse", "run"])
        os.environ["ENV_VAR_COLOR"] = "green"
        results = run_dbt(["--partial-parse", "run"])
        manifest = get_manifest(project.project_root)
        test_color_id = "test.test.check_color_model_one_env_var_ENV_VAR_COLOR___fun.89638de387"
        test_node = manifest.nodes[test_color_id]
        # kwarg was rendered but not changed (it will be rendered again when compiled)
        assert test_node.test_metadata.kwargs["color"] == "env_var('ENV_VAR_COLOR')"
        results = run_dbt(["--partial-parse", "test"])

        # Add an exposure with an env_var
        os.environ["ENV_VAR_OWNER"] = "John Doe"
        write_file(env_var_schema3_yml, project.project_root, "models", "schema.yml")
        results = run_dbt(["--partial-parse", "run"])
        manifest = get_manifest(project.project_root)
        expected_env_vars = {
            "ENV_VAR_TEST": "second",
            "TEST_SCHEMA_VAR": "view",
            "ENV_VAR_DATABASE": "dbt",
            "ENV_VAR_SEVERITY": "error",
            "ENV_VAR_COLOR": "green",
            "ENV_VAR_OWNER": "John Doe",
        }
        assert expected_env_vars == manifest.env_vars
        exposure = list(manifest.exposures.values())[0]
        schema_file = manifest.files[exposure.file_id]
        expected_sf_env_vars = {
            "models": {"model_one": ["TEST_SCHEMA_VAR", "ENV_VAR_COLOR"]},
            "exposures": {"proxy_for_dashboard": ["ENV_VAR_OWNER"]},
        }
        assert expected_sf_env_vars == schema_file.env_vars

        # add a macro and a macro schema file
        os.environ["ENV_VAR_SOME_KEY"] = "toodles"
        write_file(env_var_macro_sql, project.project_root, "macros", "env_var_macro.sql")
        write_file(env_var_macros_yml, project.project_root, "macros", "env_var_macros.yml")
        results = run_dbt(["--partial-parse", "run"])
        manifest = get_manifest(project.project_root)
        expected_env_vars = {
            "ENV_VAR_TEST": "second",
            "TEST_SCHEMA_VAR": "view",
            "ENV_VAR_DATABASE": "dbt",
            "ENV_VAR_SEVERITY": "error",
            "ENV_VAR_COLOR": "green",
            "ENV_VAR_OWNER": "John Doe",
            "ENV_VAR_SOME_KEY": "toodles",
        }
        assert expected_env_vars == manifest.env_vars
        macro_id = "macro.test.do_something"
        macro = manifest.macros[macro_id]
        assert macro.meta == {"some_key": "toodles"}
        # change the env var
        os.environ["ENV_VAR_SOME_KEY"] = "dumdedum"
        results = run_dbt(["--partial-parse", "run"])
        manifest = get_manifest(project.project_root)
        macro = manifest.macros[macro_id]
        assert macro.meta == {"some_key": "dumdedum"}

        # Add a schema file with a test on model_color and env_var in test enabled config
        write_file(env_var_model_test_yml, project.project_root, "models", "schema.yml")
        results = run_dbt(["--partial-parse", "run"])
        assert len(results) == 3
        manifest = get_manifest(project.project_root)
        model_color = manifest.nodes["model.test.model_color"]
        schema_file = manifest.files[model_color.patch_path]
        expected_env_vars = {
            "models": {
                "model_one": ["TEST_SCHEMA_VAR", "ENV_VAR_COLOR"],
                "model_color": ["ENV_VAR_ENABLED"],
            },
            "exposures": {"proxy_for_dashboard": ["ENV_VAR_OWNER"]},
        }
        assert expected_env_vars == schema_file.env_vars

        # Add a metrics file with env_vars
        os.environ["ENV_VAR_METRICS"] = "TeStInG"
        write_file(people_sql, project.project_root, "models", "people.sql")
        write_file(
            metricflow_time_spine_sql, project.project_root, "models", "metricflow_time_spine.sql"
        )
        write_file(
            people_semantic_models_yml, project.project_root, "models", "semantic_models.yml"
        )
        write_file(env_var_metrics_yml, project.project_root, "models", "metrics.yml")
        results = run_dbt(["run"])
        manifest = get_manifest(project.project_root)
        assert "ENV_VAR_METRICS" in manifest.env_vars
        assert manifest.env_vars["ENV_VAR_METRICS"] == "TeStInG"
        metric_node = manifest.metrics["metric.test.number_of_people"]
        assert metric_node.meta == {"my_meta": "TeStInG"}

        # Change metrics env var
        os.environ["ENV_VAR_METRICS"] = "Changed!"
        results = run_dbt(["run"])
        manifest = get_manifest(project.project_root)
        metric_node = manifest.metrics["metric.test.number_of_people"]
        assert metric_node.meta == {"my_meta": "Changed!"}

        # delete the env vars to cleanup
        del os.environ["ENV_VAR_TEST"]
        del os.environ["ENV_VAR_SEVERITY"]
        del os.environ["ENV_VAR_DATABASE"]
        del os.environ["TEST_SCHEMA_VAR"]
        del os.environ["ENV_VAR_COLOR"]
        del os.environ["ENV_VAR_SOME_KEY"]
        del os.environ["ENV_VAR_OWNER"]
        del os.environ["ENV_VAR_METRICS"]


class TestProjectEnvVars:
    @pytest.fixture(scope="class")
    def project_config_update(self):
        # Need to set the environment variable here initially because
        # the project fixture loads the config.
        os.environ["ENV_VAR_NAME"] = "Jane Smith"
        return {"models": {"+meta": {"meta_name": "{{ env_var('ENV_VAR_NAME') }}"}}}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "model_one.sql": model_one_sql,
        }

    def test_project_env_vars(self, project):
        # Initial run
        results = run_dbt(["run"])
        assert len(results) == 1
        manifest = get_manifest(project.project_root)
        state_check = manifest.state_check
        model_id = "model.test.model_one"
        model = manifest.nodes[model_id]
        assert model.config.meta["meta_name"] == "Jane Smith"
        env_vars_hash_checksum = state_check.project_env_vars_hash.checksum

        # Change the environment variable
        os.environ["ENV_VAR_NAME"] = "Jane Doe"
        results = run_dbt(["run"])
        assert len(results) == 1
        manifest = get_manifest(project.project_root)
        model = manifest.nodes[model_id]
        assert model.config.meta["meta_name"] == "Jane Doe"
        assert env_vars_hash_checksum != manifest.state_check.project_env_vars_hash.checksum

        # cleanup
        del os.environ["ENV_VAR_NAME"]


class TestProfileEnvVars:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "model_one.sql": model_one_sql,
        }

    @pytest.fixture(scope="class")
    def dbt_profile_target(self):
        # Need to set these here because the base integration test class
        # calls 'load_config' before the tests are run.
        # Note: only the specified profile is rendered, so there's no
        # point it setting env_vars in non-used profiles.
        os.environ["ENV_VAR_USER"] = "root"
        os.environ["ENV_VAR_PASS"] = "password"
        return {
            "type": "postgres",
            "threads": 4,
            "host": "localhost",
            "port": 5432,
            "user": "{{ env_var('ENV_VAR_USER') }}",
            "pass": "{{ env_var('ENV_VAR_PASS') }}",
            "dbname": "dbt",
        }

    def test_profile_env_vars(self, project, logs_dir):

        # Initial run
        os.environ["ENV_VAR_USER"] = "root"
        os.environ["ENV_VAR_PASS"] = "password"

        run_dbt(["run"])
        manifest = get_manifest(project.project_root)
        env_vars_checksum = manifest.state_check.profile_env_vars_hash.checksum

        # Change env_vars, the user doesn't exist, this should fail
        os.environ["ENV_VAR_USER"] = "fake_user"

        # N.B. run_dbt_and_capture won't work here because FailedToConnectError ends the test entirely
        with pytest.raises(FailedToConnectError):
            run_dbt(["run"], expect_pass=False)

        log_output = Path(logs_dir, "dbt.log").read_text()
        assert "env vars used in profiles.yml have changed" in log_output

        manifest = get_manifest(project.project_root)
        assert env_vars_checksum != manifest.state_check.profile_env_vars_hash.checksum


class TestProfileSecretEnvVars:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "model_one.sql": model_one_sql,
        }

    @property
    def dbt_profile_target(self):
        # Need to set these here because the base integration test class
        # calls 'load_config' before the tests are run.
        # Note: only the specified profile is rendered, so there's no
        # point in setting env_vars in non-used profiles.

        # user is secret and password is not. postgres on macos doesn't care if the password
        # changes so we have to change the user. related: https://github.com/dbt-labs/dbt-core/pull/4250
        os.environ[SECRET_ENV_PREFIX + "USER"] = "root"
        os.environ["ENV_VAR_PASS"] = "password"
        return {
            "type": "postgres",
            "threads": 4,
            "host": "localhost",
            "port": 5432,
            "user": "{{ env_var('DBT_ENV_SECRET_USER') }}",
            "pass": "{{ env_var('ENV_VAR_PASS') }}",
            "dbname": "dbt",
        }

    def test_profile_secret_env_vars(self, project):

        # Initial run
        os.environ[SECRET_ENV_PREFIX + "USER"] = "root"
        os.environ["ENV_VAR_PASS"] = "password"

        results = run_dbt(["run"])
        manifest = get_manifest(project.project_root)
        env_vars_checksum = manifest.state_check.profile_env_vars_hash.checksum

        # Change a secret var, it shouldn't register because we shouldn't save secrets.
        os.environ[SECRET_ENV_PREFIX + "USER"] = "fake_user"
        # we just want to see if the manifest has included
        # the secret in the hash of environment variables.
        (results, log_output) = run_dbt_and_capture(["run"], expect_pass=True)
        # I020 is the event code for "env vars used in profiles.yml have changed"
        assert not ("I020" in log_output)
        manifest = get_manifest(project.project_root)
        assert env_vars_checksum == manifest.state_check.profile_env_vars_hash.checksum
