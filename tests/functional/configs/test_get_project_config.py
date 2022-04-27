import pytest
from argparse import Namespace

from dbt.tests.util import run_dbt
from tests.functional.configs.fixtures import BaseConfigProject

from dbt.config.utils import get_project_config


class TestConfigUtils(BaseConfigProject):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "seeds": {
                "quote_columns": False,
            },
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {"my_model.sql": "select 1 as fun"}

    def test_get_project_config(
        self,
        project,
    ):
        result = run_dbt(["seed"], expect_pass=True)
        assert len(result) == 1
        result = run_dbt(["run"], expect_pass=True)
        assert len(result) == 1

        project_config = get_project_config(
            project.project_root, "test", Namespace(profiles_dir=project.profiles_dir)
        )
        assert project_config
