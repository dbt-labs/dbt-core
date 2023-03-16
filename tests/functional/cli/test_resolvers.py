import pytest
from dbt.cli.resolvers import default_log_path
from pathlib import Path


class TestDefaultLogPathNoProject:
    def test_default_log_path_no_project(self):
        expected_log_path = Path("logs")
        actual_log_path = default_log_path("nonexistent_project_dir")

        assert actual_log_path == expected_log_path


class TestDefaultLogPathWithProject:
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"log-path": "test_default_log_path"}

    def test_default_log_path_with_project(self, project, project_config_update):
        expected_log_path = Path(project.project_root) / "test_default_log_path"
        actual_log_path = default_log_path(project.project_root)

        assert actual_log_path == expected_log_path


class TestDefaultLogPathWithProjectNoConfiguredLogPath:
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"log-path": None}

    def test_default_log_path_with_project(self, project, project_config_update):
        expected_log_path = Path(project.project_root) / "logs"
        actual_log_path = default_log_path(project.project_root)

        assert actual_log_path == expected_log_path
