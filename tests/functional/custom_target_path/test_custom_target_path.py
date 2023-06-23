from pathlib import Path

import pytest

from dbt.tests.util import run_dbt


class TestTargetPathConfig:
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"config-version": 2, "target-path": "project_target"}

    def test_target_path(self, project):
        run_dbt(["run"])
        assert Path("project_target").is_dir()
        assert not Path("target").is_dir()


class TestTargetPathEnvVar:
    def test_target_path(self, project, monkeypatch):
        monkeypatch.setenv("DBT_TARGET_PATH", "env_target")
        run_dbt(["run"])
        assert Path("env_target").is_dir()
        assert not Path("project_target").is_dir()
        assert not Path("target").is_dir()


class TestTargetPathCliArg:
    def test_target_path(self, project, monkeypatch):
        monkeypatch.setenv("DBT_TARGET_PATH", "env_target")
        run_dbt(["run", "--target-path", "cli_target"])
        assert Path("cli_target").is_dir()
        assert not Path("env_target").is_dir()
        assert not Path("project_target").is_dir()
        assert not Path("target").is_dir()
