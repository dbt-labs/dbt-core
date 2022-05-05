import pytest
import os

from dbt.tests.util import run_dbt, check_relations_equal
from tests.functional.configs.fixtures import BaseConfigProject


class TestConfigs(BaseConfigProject):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "models": {
                "test": {
                    "tagged": {
                        # the model configs will override this
                        "materialized": "invalid",
                        # the model configs will append to these
                        "tags": ["tag_one"],
                    }
                },
            },
            "seeds": {
                "quote_columns": False,
            },
        }

    def test_config_layering(
        self,
        project,
    ):
        # run seed
        results = run_dbt(["seed"])
        assert len(results) == 1

        # test the project-level tag, and both config() call tags
        assert len(run_dbt(["run", "--model", "tag:tag_one"])) == 1
        assert len(run_dbt(["run", "--model", "tag:tag_two"])) == 1
        assert len(run_dbt(["run", "--model", "tag:tag_three"])) == 1
        check_relations_equal(project.adapter, ["seed", "model"])

        # make sure we overwrote the materialization properly
        tables = project.get_tables_in_schema()
        assert tables["model"] == "table"


# In addition to testing an alternative target-paths setting, it tests that
# the attribute is jinja rendered and that the context "modules" works.
class TestTargetConfigs(BaseConfigProject):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "target-path": "target_{{ modules.datetime.datetime.utcnow().strftime('%Y%m%dT%H%M%S') }}",
            "seeds": {
                "quote_columns": False,
            },
        }

    def test_alternative_target_paths(self, project):
        run_dbt(["seed"])

        target_path = ""
        for d in os.listdir("."):
            if os.path.isdir(d) and d.startswith("target_"):
                target_path = d
        assert os.path.exists(os.path.join(project.project_root, target_path, "manifest.json"))
