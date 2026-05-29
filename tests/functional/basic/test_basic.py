import pytest

from dbt.tests.util import get_manifest
from tests.functional.v2_parser_parity.v2_self_parser import run_dbt_for_mode

my_model_sql = """
  select 1 as fun
"""


@pytest.fixture(scope="class")
def models():
    return {"my_model.sql": my_model_sql}


@pytest.mark.v2_parser_parity
def test_basic(project, parser_mode):
    # Tests that a project with a single model works
    results = run_dbt_for_mode(parser_mode, ["run"])
    assert len(results) == 1
    manifest = get_manifest(project.project_root)
    assert "model.test.my_model" in manifest.nodes
