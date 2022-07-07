import os
from unittest import mock
from test.integration.base import DBTIntegrationTest, use_profile


class TestTargetPathFromProjectConfig(DBTIntegrationTest):
    @property
    def project_config(self):
        return {"config-version": 2, "target-path": "project_target"}

    @property
    def schema(self):
        return "target_path_tests_075"

    @property
    def models(self):
        return "models"

    @use_profile("postgres")
    def test_postgres_overriden_target_path(self):
        results = self.run_dbt(args=["run"])
        self.assertFalse(os.path.exists("./target"))
        self.assertTrue(os.path.exists("./project_target"))


class TestTargetPathOverridenEnv(TestTargetPathFromProjectConfig):
    @use_profile("postgres")
    def test_postgres_overriden_target_path(self):
        with mock.patch.dict(os.environ, {"DBT_TARGET_PATH": "env_target"}):
            results = self.run_dbt(args=["run"])
        self.assertFalse(os.path.exists("./target"))
        self.assertFalse(os.path.exists("./project_target"))
        self.assertTrue(os.path.exists("./env_target"))


class TestTargetPathOverridenEnvironment(TestTargetPathFromProjectConfig):
    @use_profile("postgres")
    def test_postgres_overriden_target_path(self):
        with mock.patch.dict(os.environ, {"DBT_TARGET_PATH": "env_target"}):
            results = self.run_dbt(args=["run", "--target-path", "cli_target"])
        self.assertFalse(os.path.exists("./target"))
        self.assertFalse(os.path.exists("./project_target"))
        self.assertFalse(os.path.exists("./env_target"))
        self.assertTrue(os.path.exists("./cli_target"))
