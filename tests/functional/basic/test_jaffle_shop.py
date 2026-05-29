import pytest

from dbt.tests.util import get_manifest, write_file
from tests.fixtures.jaffle_shop import JaffleShopProject
from tests.functional.v2_parser_parity.v2_self_parser import (
    run_dbt_and_capture_for_mode,
    run_dbt_for_mode,
)


class TestBasic(JaffleShopProject):
    @pytest.mark.v2_parser_parity
    def test_basic(self, project, parser_mode):
        # test .dbtignore works
        write_file("models/ignore*.sql\nignore_folder", project.project_root, ".dbtignore")
        # Create the data from seeds
        results = run_dbt_for_mode(parser_mode, ["seed"])

        # Tests that the jaffle_shop project runs
        results = run_dbt_for_mode(parser_mode, ["run"])
        assert len(results) == 5
        manifest = get_manifest(project.project_root)
        assert "model.jaffle_shop.orders" in manifest.nodes

    @pytest.mark.v2_parser_parity
    def test_execution_time_format_is_humanized(self, project, parser_mode):
        # Create the data from seeds
        run_dbt_for_mode(parser_mode, ["seed"])
        _, log_output = run_dbt_and_capture_for_mode(parser_mode, ["run"])

        assert " in 0 hours 0 minutes and " in log_output
        assert " seconds" in log_output
