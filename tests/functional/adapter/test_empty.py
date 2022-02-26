from dbt.tests.util import run_dbt
import os


def test_empty(project):
    # check seed
    results = run_dbt(["seed"])
    assert len(results) == 0
    run_results_path = os.path.join(project.project_root, "target", "run_results.json")
    assert os.path.exists(run_results_path)

    # check run
    results = run_dbt(["run"])
    assert len(results) == 0

    catalog_path = os.path.join(project.project_root, "target", "catalog.json")
    assert not os.path.exists(catalog_path)

    # check catalog
    catalog = run_dbt(["docs", "generate"])
    assert os.path.exists(run_results_path)
    assert os.path.exists(catalog_path)
    assert len(catalog.nodes) == 0
    assert len(catalog.sources) == 0
